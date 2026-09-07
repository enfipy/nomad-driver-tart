package driver

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

// No task environment ever reaches a host command. In particular disable
// Tart's automatic pruning and never use a user's Tart store or keychain.
func (c Config) hostEnv() []string {
	return []string{"HOME=" + c.StateDir, "TART_HOME=" + filepath.Join(c.StateDir, "vms"), "TART_NO_AUTO_PRUNE=1", "PATH=" + c.SoftnetDir + ":/usr/bin:/bin:/usr/sbin:/sbin", "TMPDIR=" + filepath.Join(c.StateDir, "tmp")}
}
func (d *Driver) command(ctx context.Context, args ...string) *exec.Cmd {
	cmd := exec.CommandContext(ctx, d.config.TartPath, args...)
	cmd.Env = d.config.hostEnv()
	cmd.Dir = d.config.StateDir
	cmd.WaitDelay = 5 * time.Second
	return cmd
}
func (d *Driver) small(ctx context.Context, args ...string) ([]byte, error) {
	var b limitedBuffer
	cmd := d.command(ctx, args...)
	cmd.Stdout = &b
	cmd.Stderr = io.Discard
	if e := cmd.Run(); e != nil {
		return nil, fmt.Errorf("tart %s failed: %w", args[0], e)
	}
	return b.data, nil
}

type limitedBuffer struct{ data []byte }

func (b *limitedBuffer) Write(p []byte) (int, error) {
	if len(b.data)+len(p) > 1024*1024 {
		return 0, fmt.Errorf("observation exceeds 1 MiB")
	}
	b.data = append(b.data, p...)
	return len(p), nil
}

type cappedWriter struct {
	w         io.Writer
	remaining int64
	cancel    context.CancelFunc
}

func (w *cappedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		w.cancel()
		return 0, fmt.Errorf("output limit exceeded")
	}
	n, e := w.w.Write(p)
	w.remaining -= int64(n)
	return n, e
}

func (d *Driver) vmState(ctx context.Context, name string) (string, error) {
	b, e := d.small(ctx, "list", "--format", "json")
	if e != nil {
		return "", e
	}
	var entries []struct{ Name, Source, State string }
	if e = json.Unmarshal(b, &entries); e != nil {
		return "", e
	}
	for _, v := range entries {
		if v.Name == name && v.Source == "local" {
			return strings.ToLower(v.State), nil
		}
	}
	return "absent", nil
}
func (d *Driver) cleanup(r *record) error {
	if e := r.validate(taskKey(r.TaskID)); e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	state, e := d.vmState(ctx, r.VM)
	if e != nil {
		return e
	}
	if state == "running" || state == "suspended" {
		if _, e = d.small(ctx, "stop", r.VM, "--timeout", "10"); e != nil {
			return e
		}
	}
	for i := 0; i < 50; i++ {
		state, e = d.vmState(ctx, r.VM)
		if e != nil {
			return e
		}
		if state == "stopped" || state == "absent" {
			break
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(100 * time.Millisecond):
		}
	}
	if state != "stopped" && state != "absent" {
		return fmt.Errorf("owned VM still running")
	}
	if state != "absent" {
		if _, e = d.small(ctx, "delete", r.VM); e != nil {
			return e
		}
	}
	state, e = d.vmState(ctx, r.VM)
	if e != nil {
		return e
	}
	if state != "absent" {
		return fmt.Errorf("owned VM deletion not verified")
	}
	r.CleanupPending = false
	return nil
}

func (d *Driver) runTask(h *taskHandle, tc TaskConfig) {
	defer close(h.done)
	ctx := h.ctx
	// The record exists before any VM mutation. Every exit converges through
	// cleanup, including log setup failure, cloning and guest RPC cancellation.
	defer func() {
		h.mu.Lock()
		h.r.Phase = "cleaning"
		r := h.r
		h.mu.Unlock()
		if e := d.save(r); e != nil {
			r.Failure = "persisting build result failed: " + e.Error()
		}
		if e := d.cleanup(&r); e != nil {
			r.Failure = "cleanup pending: " + e.Error()
			r.CleanupPending = true
		}
		r.Phase = "complete"
		if r.CleanupPending {
			r.Phase = "cleanup_pending"
		}
		r.Finished = time.Now()
		if e := d.save(r); e != nil {
			r.Failure = "persisting terminal state failed: " + e.Error()
			r.CleanupPending = true
			r.Phase = "cleanup_pending"
		}
		h.mu.Lock()
		h.r = r
		h.mu.Unlock()
		h.cancel()
		d.mu.Lock()
		d.busy = r.CleanupPending
		d.mu.Unlock()
		d.event(h, r.Phase)
	}()
	fail := func(e error) { h.mu.Lock(); h.r.Failure = e.Error(); h.mu.Unlock() }
	out, e := os.OpenFile(h.cfg.StdoutPath, os.O_WRONLY, 0)
	if e != nil {
		fail(e)
		return
	}
	defer out.Close()
	errout, e := os.OpenFile(h.cfg.StderrPath, os.O_WRONLY, 0)
	if e != nil {
		fail(e)
		return
	}
	defer errout.Close()
	stdout := &cappedWriter{out, 64 << 20, h.cancel}
	stderr := &cappedWriter{errout, 64 << 20, h.cancel}
	if _, e = d.small(ctx, "clone", d.config.Image, h.r.VM); e != nil {
		fail(e)
		return
	}
	if _, e = d.small(ctx, "set", h.r.VM, "--cpu", strconv.Itoa(d.config.VCPUs), "--memory", strconv.FormatInt(d.config.MemoryMB, 10)); e != nil {
		fail(e)
		return
	}
	vm := d.command(ctx, "run", h.r.VM, "--no-graphics", "--no-clipboard", "--no-audio", "--net-softnet", "--net-softnet-block", strings.Join(d.config.Block, ","))
	if len(d.config.Allow) > 0 {
		vm.Args = append(vm.Args, "--net-softnet-allow", strings.Join(d.config.Allow, ","))
	}
	vm.Stdout = io.Discard
	vm.Stderr = stderr
	if e = vm.Start(); e != nil {
		fail(e)
		return
	}
	h.mu.Lock()
	h.pid = vm.Process.Pid
	h.mu.Unlock()
	vmDone := make(chan error, 1)
	go func() { vmDone <- vm.Wait(); h.cancel() }()
	defer func() { // Bound waiting even when the VM helper misbehaves.
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer stopCancel()
		if _, e := d.small(stopCtx, "stop", h.r.VM, "--timeout", "10"); e != nil {
			_ = vm.Process.Kill()
		}
		select {
		case <-vmDone:
		case <-time.After(15 * time.Second):
			_ = vm.Process.Kill()
		}
	}()
	ready, readyCancel := context.WithTimeout(ctx, 120*time.Second)
	for {
		if _, e = d.small(ready, "exec", h.r.VM, "/usr/bin/true"); e == nil {
			break
		}
		select {
		case <-ready.Done():
			readyCancel()
			fail(fmt.Errorf("guest agent readiness: %w", ready.Err()))
			return
		case <-time.After(time.Second):
		}
	}
	readyCancel()
	if tc.Source {
		source, e := openRegular(filepath.Join(h.cfg.TaskDir().LocalDir, "source.tar"), 1<<30)
		if e != nil {
			fail(e)
			return
		}
		defer source.Close()
		cmd := d.command(ctx, "exec", "-i", h.r.VM, "/bin/sh", "-c", "mkdir -p /tmp/cloud-build && cd /tmp/cloud-build && tar -xf -")
		cmd.Stdin = source
		cmd.Stdout = stdout
		cmd.Stderr = stderr
		if e = cmd.Run(); e != nil {
			fail(e)
			return
		}
	}
	h.mu.Lock()
	h.r.Phase = "building"
	h.mu.Unlock()
	d.event(h, "building")
	// Workload environment is deliberately not forwarded: source/argv contain
	// the build contract, and images supply the toolchain. No Nomad tokens enter.
	cmd := d.command(ctx, append([]string{"exec", h.r.VM, tc.Command}, tc.Args...)...)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	e = cmd.Run()
	code := 0
	if e != nil {
		var exit *exec.ExitError
		if errors.As(e, &exit) && ctx.Err() == nil {
			code = exit.ExitCode()
		} else {
			fail(fmt.Errorf("build interrupted: %w", e))
			return
		}
	}
	h.mu.Lock()
	h.r.ExitCode = code
	h.r.Phase = "collecting"
	h.mu.Unlock()
	d.event(h, "collecting")
	if tc.Artifacts {
		if e = d.collect(ctx, h); e != nil {
			fail(e)
			return
		}
	}
}

func (d *Driver) collect(ctx context.Context, h *taskHandle) error {
	dir := h.cfg.TaskDir().LocalDir
	if e := safeDir(dir); e != nil {
		return e
	}
	name := h.r.VM + ".tar"
	path := filepath.Join(dir, name)
	f, e := os.OpenFile(path+".partial", os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if e != nil {
		return e
	}
	defer f.Close()
	defer os.Remove(path + ".partial")
	sum := sha256.New()
	cmd := d.command(ctx, "exec", h.r.VM, "/usr/bin/tar", "-C", "/tmp/cloud-artifacts", "-cf", "-", ".")
	cmd.Stdout = &cappedWriter{io.MultiWriter(f, sum), d.config.ArtifactMB << 20, h.cancel}
	cmd.Stderr = io.Discard
	if e = cmd.Run(); e != nil {
		return fmt.Errorf("artifact export failed: %w", e)
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
	h.mu.Lock()
	h.r.Artifact = name
	h.r.ArtifactSHA256 = hex.EncodeToString(sum.Sum(nil))
	r := h.r
	h.mu.Unlock()
	return writeAtomic(filepath.Join(dir, "build-result.json"), r)
}
