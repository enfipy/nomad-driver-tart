package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/nomad/client/lib/cpustats"
	"github.com/hashicorp/nomad/drivers/shared/executor"
	"github.com/hashicorp/nomad/plugins/drivers"
	pstructs "github.com/hashicorp/nomad/plugins/shared/structs"
)

// buildStore contains only the restrictive profile's durable ownership and
// admission state. Task lifecycle remains in Driver/taskHandle and Client.
type buildStore struct {
	lock, lifeReader, lifeWriter *os.File
	busy                         bool
}

func (c BuildConfig) hostEnv() []string {
	return []string{"HOME=" + c.StateDir, "TART_HOME=" + filepath.Join(c.StateDir, "vms"), "TART_NO_AUTO_PRUNE=1", "PATH=" + c.SoftnetDir + ":/usr/bin:/bin:/usr/sbin:/sbin", "TMPDIR=" + filepath.Join(c.StateDir, "tmp")}
}

type buildRunner struct {
	profile *BuildConfig
	store   *buildStore
}

func (r buildRunner) Run(ctx context.Context, _ string, args ...string) *exec.Cmd {
	binary, err := os.Executable()
	if err != nil {
		panic(err)
	}
	cmd := exec.CommandContext(ctx, binary, append([]string{"--cloud-tart-child", r.profile.TartPath}, args...)...)
	cmd.ExtraFiles = []*os.File{r.store.lifeReader, r.store.lock}
	cmd.Cancel = func() error { return cmd.Process.Signal(syscall.SIGTERM) }
	cmd.WaitDelay = 5 * time.Second
	cmd.Env = r.profile.hostEnv()
	cmd.Dir = r.profile.StateDir
	return cmd
}
func (d *Driver) configureBuild() (err error) {
	c := d.config.Build
	if runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" || os.Geteuid() == 0 {
		return fmt.Errorf("unprivileged Apple Silicon service account required")
	}
	if e := privateDir(c.StateDir); e != nil {
		return e
	}
	for _, path := range []string{c.TartPath, filepath.Join(c.SoftnetDir, "softnet")} {
		real, e := filepath.EvalSymlinks(path)
		if e != nil {
			return e
		}
		for p := real; p != "/"; p = filepath.Dir(p) {
			s, e := os.Stat(p)
			if e != nil {
				return e
			}
			if s.Mode().Perm()&0022 != 0 || s.Sys().(*syscall.Stat_t).Uid != 0 {
				return fmt.Errorf("immutable root-owned executable ancestry required")
			}
		}
	}
	for _, name := range []string{"records", "vms", "tmp"} {
		path := filepath.Join(c.StateDir, name)
		if e := os.Mkdir(path, 0700); e != nil && !os.IsExist(e) {
			return e
		}
		if e := privateDir(path); e != nil {
			return e
		}
	}
	store := &buildStore{}
	store.lock, err = os.OpenFile(filepath.Join(c.StateDir, "driver.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if err != nil {
			store.lock.Close()
			if store.lifeReader != nil {
				store.lifeReader.Close()
				store.lifeWriter.Close()
			}
			d.build = nil
		}
	}()
	if err = syscall.Flock(int(store.lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		return fmt.Errorf("old driver/helpers still own VM store")
	}
	store.lifeReader, store.lifeWriter, err = os.Pipe()
	if err != nil {
		return err
	}
	client, ok := d.client.(*tartCLI)
	if !ok {
		return fmt.Errorf("build profile requires Tart CLI client")
	}
	oldRunner := client.runner
	client.runner = buildRunner{c, store}
	d.build = store
	defer func() {
		if err != nil {
			client.runner = oldRunner
		}
	}()
	return d.reconcileBuilds()
}
func (d *Driver) reconcileBuilds() error {
	entries, e := os.ReadDir(filepath.Join(d.config.Build.StateDir, "records"))
	if e != nil {
		return e
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".record-") {
			continue
		}
		if !strings.HasSuffix(entry.Name(), ".json") {
			return fmt.Errorf("unknown ownership entry")
		}
		r, e := readRecord(filepath.Join(d.config.Build.StateDir, "records", entry.Name()))
		if e != nil {
			return e
		}
		if r.CleanupPending {
			if e = d.cleanupBuild(&r); e != nil {
				return e
			}
			r.Failure = "build interrupted by driver/agent restart"
			r.ExitCode = -1
			r.Phase = "complete"
			r.Finished = time.Now()
			if e = d.save(r); e != nil {
				return e
			}
		}
	}
	return nil
}

// cleanupVM is shared by startup failure and build completion. Only the exact
// owned destination is stopped/deleted; image caches and foreign VMs are untouched.
func (d *Driver) cleanupVM(name string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	find := func() (VMState, bool, error) {
		vms, e := d.client.List(ctx)
		if e != nil {
			return "", false, e
		}
		for _, v := range vms {
			if v.Name == name && v.Source != "oci" {
				return v.Status, true, nil
			}
		}
		return "", false, nil
	}
	state, found, e := find()
	if e != nil || !found {
		return e
	}
	if state == VMStateRunning || state == VMStatePaused {
		if e = d.client.Stop(ctx, name, 10*time.Second); e != nil {
			return e
		}
	}
	for i := 0; i < 50; i++ {
		state, found, e = find()
		if e != nil {
			return e
		}
		if !found {
			return nil
		}
		if state == VMStateStopped {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	if state != VMStateStopped {
		return fmt.Errorf("owned VM still running")
	}
	if e = d.client.Delete(ctx, name); e != nil {
		return e
	}
	_, found, e = find()
	if e != nil {
		return e
	}
	if found {
		return fmt.Errorf("owned VM deletion not verified")
	}
	return nil
}
func (d *Driver) cleanupBuild(r *buildRecord) error {
	if e := r.validate(taskKey(r.TaskID)); e != nil {
		return e
	}
	if r.Reattach != nil {
		// Never signal an unverified PID: reattach through the persisted executor
		// endpoint and validate its RPC protocol before asking it to shut down.
		if e := syscall.Kill(r.Reattach.Pid, 0); e == nil {
			rc, e := pstructs.ReattachConfigToGoPlugin(r.Reattach)
			if e != nil {
				return e
			}
			ex, pc, e := executor.ReattachToExecutor(rc, d.logger, cpustats.Compute{})
			if e != nil {
				return e
			}
			e = ex.Shutdown("SIGINT", 10*time.Second)
			pc.Kill()
			if e != nil {
				return e
			}
		} else if e != syscall.ESRCH {
			return e
		}
	}
	if e := d.cleanupVM(r.VM); e != nil {
		return e
	}
	r.Reattach = nil
	r.CleanupPending = false
	return nil
}
func buildResult(r buildRecord) *drivers.ExitResult {
	result := &drivers.ExitResult{ExitCode: r.ExitCode}
	if r.Failure != "" {
		result.Err = fmt.Errorf("%s", r.Failure)
	}
	return result
}
func (d *Driver) finishBuild(h *taskHandle, exited *drivers.ExitResult) *drivers.ExitResult {
	h.stateLock.Lock()
	r := *h.build
	h.stateLock.Unlock()
	if r.ExitCode < 0 && r.Failure == "" {
		if exited.Err != nil {
			r.Failure = exited.Err.Error()
		} else {
			r.Failure = "VM exited before build completed"
		}
	}
	// run() has joined the guest goroutine and VM executor before this point.
	if h.pluginClient != nil {
		h.pluginClient.Kill()
	}
	r.Reattach = nil
	r.Phase = "cleaning"
	if e := d.save(r); e != nil {
		r.Failure = "persisting build result: " + e.Error()
	}
	if e := d.cleanupBuild(&r); e != nil {
		r.Failure = "cleanup pending: " + e.Error()
		r.CleanupPending = true
	}
	r.Phase = "complete"
	if r.CleanupPending {
		r.Phase = "cleanup_pending"
	}
	r.Finished = time.Now()
	if e := d.save(r); e != nil {
		r.Failure = "persisting terminal state: " + e.Error()
		r.CleanupPending = true
		r.Phase = "cleanup_pending"
	}
	if e := publishBuildResult(h.taskConfig, r); e != nil {
		r.Failure = strings.TrimPrefix(r.Failure+"; publishing build result: "+e.Error(), "; ")
		if e = d.save(r); e != nil {
			r.Failure += "; persisting terminal state: " + e.Error()
			r.CleanupPending = true
			r.Phase = "cleanup_pending"
		}
	}
	h.stateLock.Lock()
	*h.build = r
	h.stateLock.Unlock()
	d.admission.Lock()
	d.build.busy = r.CleanupPending
	d.admission.Unlock()
	d.buildEvent(h, r.Phase)
	return buildResult(r)
}
func (d *Driver) buildEvent(h *taskHandle, phase string) {
	h.stateLock.Lock()
	h.build.Phase = phase
	r := *h.build
	h.stateLock.Unlock()
	d.emitTaskEvent(h.taskConfig, "Tart build: "+phase, map[string]string{"tart.phase": phase, "tart.vm": r.VM, "tart.image": r.Image, "tart.artifact": r.Artifact, "tart.artifact_sha256": r.ArtifactSHA256})
}

type writeCloser struct{ io.Writer }

func (writeCloser) Close() error { return nil }

type cappedWriter struct {
	mu        sync.Mutex
	w         io.Writer
	remaining int64
	cancel    context.CancelFunc
}

func (w *cappedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if int64(len(p)) > w.remaining {
		w.cancel()
		return 0, fmt.Errorf("output limit exceeded")
	}
	n, e := w.w.Write(p)
	w.remaining -= int64(n)
	return n, e
}
func (d *Driver) waitForGuestAgent(ctx context.Context, vm VMConfig) error {
	ready, cancel := context.WithTimeout(ctx, 120*time.Second)
	defer cancel()
	for {
		if code, e := d.client.Exec(ready, vm, ExecOptions{Command: []string{"/usr/bin/true"}, Stdout: writeCloser{io.Discard}, Stderr: writeCloser{io.Discard}}); e == nil && code == 0 {
			return nil
		}
		select {
		case <-ready.Done():
			return ready.Err()
		case <-time.After(time.Second):
		}
	}
}
func (d *Driver) executeBuild(ctx context.Context, h *taskHandle) {
	fail := h.failBuild
	out, e := openLog(ctx, h.taskConfig.StdoutPath)
	if e != nil {
		fail(e)
		return
	}
	defer out.Close()
	errout, e := openLog(ctx, h.taskConfig.StderrPath)
	if e != nil {
		fail(e)
		return
	}
	defer errout.Close()
	stdout := writeCloser{&cappedWriter{w: logWriter{ctx, out}, remaining: 64 << 20, cancel: h.shutdown}}
	stderr := writeCloser{&cappedWriter{w: logWriter{ctx, errout}, remaining: 64 << 20, cancel: h.shutdown}}
	vm := h.vmConfig
	if e = d.waitForGuestAgent(ctx, vm); e != nil {
		fail(fmt.Errorf("guest readiness: %w", e))
		return
	}
	if vm.Driver.Source {
		source, e := openRegular(filepath.Join(h.taskConfig.TaskDir().LocalDir, "source.tar"), 1<<30)
		if e != nil {
			fail(e)
			return
		}
		defer source.Close()
		code, e := d.client.Exec(ctx, vm, ExecOptions{Command: []string{"/bin/sh", "-c", "mkdir -p /tmp/cloud-build && cd /tmp/cloud-build && tar -xf -"}, Stdin: source, Stdout: stdout, Stderr: stderr})
		if e != nil || code != 0 {
			fail(fmt.Errorf("source import failed: exit %d: %v", code, e))
			return
		}
	}
	d.buildEvent(h, "building")
	code, e := d.client.Exec(ctx, vm, ExecOptions{Command: append([]string{vm.Driver.Command}, vm.Driver.Args...), Stdout: stdout, Stderr: stderr})
	if e != nil {
		fail(e)
		return
	}
	h.stateLock.Lock()
	h.build.ExitCode = code
	h.stateLock.Unlock()
	d.buildEvent(h, "collecting")
	if vm.Driver.Artifacts {
		if e = d.collectBuild(ctx, h); e != nil {
			fail(e)
		}
	}
}
func (d *Driver) collectBuild(ctx context.Context, h *taskHandle) error {
	dir := h.taskConfig.TaskDir().LocalDir
	if e := safeDir(dir); e != nil {
		return e
	}
	name := h.vmName() + ".tar"
	path := filepath.Join(dir, name)
	f, e := os.OpenFile(path+".partial", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	defer os.Remove(path + ".partial")
	sum := sha256.New()
	output := writeCloser{&cappedWriter{w: io.MultiWriter(f, sum), remaining: h.vmConfig.Build.ArtifactMB << 20, cancel: h.shutdown}}
	code, e := d.client.Exec(ctx, h.vmConfig, ExecOptions{Command: []string{"/usr/bin/tar", "-C", "/tmp/cloud-artifacts", "-cf", "-", "."}, Stdout: output, Stderr: writeCloser{io.Discard}})
	if e != nil || code != 0 {
		return fmt.Errorf("artifact export failed: exit %d: %v", code, e)
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(path+".partial", path); e != nil {
		return e
	}
	h.stateLock.Lock()
	h.build.Artifact = name
	h.build.ArtifactSHA256 = hex.EncodeToString(sum.Sum(nil))
	r := *h.build
	h.stateLock.Unlock()
	return d.save(r)
}

// Build samples include the attributed Virtualization helper and Tart's process
// tree. Missing/ambiguous identity produces no fresh sample, never a false zero.
func (d *Driver) buildStats(ctx context.Context, h *taskHandle, interval time.Duration) (<-chan *drivers.TaskResourceUsage, error) {
	ch := make(chan *drivers.TaskResourceUsage)
	if interval < time.Second {
		interval = time.Second
	}
	go func() {
		defer close(ch)
		var counter buildUsageCounter
		for {
			select {
			case <-ctx.Done():
				return
			case <-h.doneCh:
				return
			default:
			}
			h.stateLock.RLock()
			pid, started := h.pid, h.startedAt
			h.stateLock.RUnlock()
			if pid > 0 {
				sampleCtx, cancel := context.WithTimeout(ctx, 4*time.Second)
				vms, err := d.client.List(sampleCtx)
				if err == nil && soleRunningBuildVM(vms, h.vmConfig.Name) {
					ps, err := readBuildProcesses(sampleCtx, int32(pid), d.config.Build.TartPath, started)
					if err == nil {
						sample, err := counter.sample(ps, time.Now())
						if err == nil {
							select {
							case ch <- sample:
							case <-ctx.Done():
								cancel()
								return
							case <-h.doneCh:
								cancel()
								return
							}
						}
					}
				}
				cancel()
			}
			select {
			case <-ctx.Done():
				return
			case <-h.doneCh:
				return
			case <-time.After(interval):
			}
		}
	}()
	return ch, nil
}

// Preserve the initiating error when cancellation produces secondary failures.
func (h *taskHandle) failBuild(e error) {
	h.stateLock.Lock()
	defer h.stateLock.Unlock()
	if h.build.Failure == "" {
		h.build.Failure = e.Error()
	}
}

func publishBuildResult(cfg *drivers.TaskConfig, r buildRecord) error {
	dir := cfg.TaskDir().LocalDir
	if e := safeDir(dir); e != nil {
		return e
	}
	return writeAtomic(filepath.Join(dir, "build-result.json"), r)
}
