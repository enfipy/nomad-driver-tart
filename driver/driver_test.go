package driver

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/nomad/plugins/drivers"
)

func fixture(t *testing.T) (*Driver, *drivers.TaskConfig) {
	t.Helper()
	root, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	c := Config{Enabled: true, TartPath: filepath.Join(root, "tart"), SoftnetDir: root, StateDir: root, Image: "ghcr.io/test/image@sha256:" + strings.Repeat("a", 64), Xcode: "26.5", CPU: 20000, VCPUs: 4, MemoryMB: 8192, OverheadMB: 1024, TimeoutSeconds: 30, ArtifactMB: 1, Jobs: []string{"qualify"}, Block: []string{"0.0.0.0/0", "@host"}}
	for _, p := range []string{"records", "vms", "tmp", "alloc/build/local"} {
		if e = os.MkdirAll(filepath.Join(root, p), 0700); e != nil {
			t.Fatal(e)
		}
	}
	if e = os.WriteFile(c.TartPath, []byte(fakeTart), 0700); e != nil {
		t.Fatal(e)
	}
	out := filepath.Join(root, "stdout")
	errout := filepath.Join(root, "stderr")
	os.WriteFile(out, nil, 0600)
	os.WriteFile(errout, nil, 0600)
	d := NewTartDriver(hclog.NewNullLogger()).(*Driver)
	d.config = c
	cfg := &drivers.TaskConfig{ID: "task1", AllocID: "alloc1", JobName: "qualify", JobID: "qualify", Namespace: "canary", Name: "build", AllocDir: filepath.Join(root, "alloc"), StdoutPath: out, StderrPath: errout, Resources: &drivers.Resources{NomadResources: &structs.AllocatedTaskResources{Cpu: structs.AllocatedCpuResources{CpuShares: 20000}, Memory: structs.AllocatedMemoryResources{MemoryMB: 9216}}}}
	return d, cfg
}
func start(t *testing.T, d *Driver, cfg *drivers.TaskConfig, command string) *drivers.TaskHandle {
	t.Helper()
	if e := cfg.EncodeConcreteDriverConfig(TaskConfig{Command: command, Artifacts: true}); e != nil {
		t.Fatal(e)
	}
	h, _, e := d.StartTask(cfg)
	if e != nil {
		t.Fatal(e)
	}
	return h
}
func wait(t *testing.T, d *Driver, id string) *drivers.ExitResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()
	ch, e := d.WaitTask(ctx, id)
	if e != nil {
		t.Fatal(e)
	}
	select {
	case r := <-ch:
		if r == nil {
			t.Fatal("missing result")
		}
		return r
	case <-ctx.Done():
		t.Fatal("task timed out")
		return nil
	}
}

func TestBuildExitArtifactsAndHostEnvironment(t *testing.T) {
	d, cfg := fixture(t)
	cfg.Env = map[string]string{"TART_HOME": "/unowned", "DYLD_INSERT_LIBRARIES": "/evil", "PATH": "/evil", "NOMAD_TOKEN": "secret"}
	start(t, d, cfg, "/guest/fail")
	r := wait(t, d, cfg.ID)
	if r.ExitCode != 7 || r.Err != nil {
		t.Fatalf("guest exit not preserved: %+v", r)
	}
	state, e := readRecord(d.recordPath(cfg.ID))
	if e != nil {
		t.Fatal(e)
	}
	if state.CleanupPending || state.ArtifactSHA256 == "" {
		t.Fatalf("incomplete terminal record: %+v", state)
	}
	bytes, e := os.ReadFile(filepath.Join(cfg.TaskDir().LocalDir, state.Artifact))
	if e != nil || string(bytes) != "artifact bytes" {
		t.Fatalf("artifact: %q %v", bytes, e)
	}
	hostenv, _ := os.ReadFile(filepath.Join(d.config.StateDir, "environment"))
	if strings.Contains(string(hostenv), "secret") || strings.Contains(string(hostenv), "/evil") {
		t.Fatal("task environment reached host")
	}
	stdout, _ := os.ReadFile(cfg.StdoutPath)
	if !strings.Contains(string(stdout), "build output") {
		t.Fatal("guest logs missing")
	}
	if e = d.DestroyTask(cfg.ID, false); e != nil {
		t.Fatal(e)
	}
}
func TestAdmissionAndResourceValidation(t *testing.T) {
	d, cfg := fixture(t)
	if e := d.config.validate(); e != nil {
		t.Fatal(e)
	}
	for _, mutate := range []func(*drivers.TaskConfig){func(c *drivers.TaskConfig) { c.User = "root" }, func(c *drivers.TaskConfig) { c.Namespace = "default" }, func(c *drivers.TaskConfig) { c.JobID = "untrusted" }, func(c *drivers.TaskConfig) { c.Resources.NomadResources.Cpu.CpuShares = 100 }, func(c *drivers.TaskConfig) { c.Resources.NomadResources.Memory.MemoryMB = 8192 }} {
		copy := cfg.Copy()
		mutate(copy)
		if e := d.config.validateTask(copy, TaskConfig{Command: "true"}); e == nil {
			t.Fatal("unsafe request accepted")
		}
	}
	d.mu.Lock()
	d.busy = true
	d.mu.Unlock()
	cfg.EncodeConcreteDriverConfig(TaskConfig{Command: "true"})
	if _, _, e := d.StartTask(cfg); e == nil {
		t.Fatal("concurrent admission accepted")
	}
}
func TestCleanupFailureRemainsRecoverable(t *testing.T) {
	d, cfg := fixture(t)
	os.WriteFile(filepath.Join(d.config.StateDir, "fail-delete"), nil, 0600)
	start(t, d, cfg, "/guest/ok")
	r := wait(t, d, cfg.ID)
	if r.Err == nil {
		t.Fatal("cleanup failure reported success")
	}
	if e := d.DestroyTask(cfg.ID, false); e == nil {
		t.Fatal("forgot failed cleanup")
	}
	if _, e := os.Stat(d.recordPath(cfg.ID)); e != nil {
		t.Fatal("ownership lost")
	}
	os.Remove(filepath.Join(d.config.StateDir, "fail-delete"))
	restarted := NewTartDriver(hclog.NewNullLogger()).(*Driver)
	restarted.config = d.config
	if e := restarted.reconcile(); e != nil {
		t.Fatal(e)
	}
	r2, e := readRecord(d.recordPath(cfg.ID))
	if e != nil || r2.CleanupPending {
		t.Fatalf("reconcile failed: %+v %v", r2, e)
	}
	calls, _ := os.ReadFile(filepath.Join(d.config.StateDir, "calls"))
	if strings.Count(string(calls), "clone\n") != 1 {
		t.Fatal("recovery replayed clone")
	}
}
func TestRecoveryDoesNotReplayAndUnknownRecordFailsClosed(t *testing.T) {
	d, cfg := fixture(t)
	r := newRecord(cfg.ID, cfg.AllocID, d.config.Image)
	if e := d.save(r); e != nil {
		t.Fatal(e)
	}
	if e := d.reconcile(); e != nil {
		t.Fatal(e)
	}
	h := drivers.NewTaskHandle(2)
	h.Config = cfg
	if e := d.RecoverTask(h); e != nil {
		t.Fatal(e)
	}
	result := wait(t, d, cfg.ID)
	if result.Err == nil || result.ExitCode != -1 {
		t.Fatalf("interrupted build became success: %+v", result)
	}
	r.VM = "user-vm"
	b, _ := json.Marshal(r)
	os.WriteFile(d.recordPath(cfg.ID), b, 0600)
	if e := d.reconcile(); e == nil {
		t.Fatal("foreign VM record accepted")
	}
}
func TestCancellationStopsBackgroundVM(t *testing.T) {
	d, cfg := fixture(t)
	start(t, d, cfg, "/guest/hang")
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if _, e := os.Stat(filepath.Join(d.config.StateDir, "guest-started")); e == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if e := d.StopTask(cfg.ID, time.Second, "SIGTERM"); e != nil {
		t.Fatal(e)
	}
	r := wait(t, d, cfg.ID)
	if r.Err == nil {
		t.Fatal("cancelled build succeeded")
	}
	if _, e := os.Stat(filepath.Join(d.config.StateDir, "vm-name")); !os.IsNotExist(e) {
		t.Fatal("VM survived cancellation")
	}
}
func TestRegularSourceAndArtifactLimits(t *testing.T) {
	d, _ := fixture(t)
	path := filepath.Join(d.config.StateDir, "link")
	os.Symlink("/etc/passwd", path)
	if f, e := openRegular(path, 1<<20); e == nil {
		f.Close()
		t.Fatal("source symlink accepted")
	}
	ctx, cancel := context.WithCancel(context.Background())
	w := cappedWriter{w: &strings.Builder{}, remaining: 3, cancel: cancel}
	if _, e := w.Write([]byte("four")); e == nil || ctx.Err() == nil {
		t.Fatal("output limit did not cancel")
	}
}

const fakeTart = `#!/bin/sh
set -eu
echo "$1" >> "$HOME/calls"
case "$1" in
 --version) echo '2.35.0';;
 clone) echo "$3" > "$HOME/vm-name"; echo stopped > "$HOME/vm-state"; env > "$HOME/environment";;
 set) :;;
 list) if test -f "$HOME/vm-name"; then printf '[{"Name":"%s","Source":"local","State":"%s"}]' "$(cat "$HOME/vm-name")" "$(cat "$HOME/vm-state")"; else echo '[]'; fi;;
 run) echo running > "$HOME/vm-state"; echo $$ > "$HOME/vm-pid"; trap 'echo stopped > "$HOME/vm-state"; exit 0' TERM INT; while :; do sleep 0.1; done;;
 stop) if test -f "$HOME/vm-pid"; then kill -TERM "$(cat "$HOME/vm-pid")" 2>/dev/null || true; fi; echo stopped > "$HOME/vm-state";;
 delete) test ! -f "$HOME/fail-delete"; rm -f "$HOME/vm-name" "$HOME/vm-state" "$HOME/vm-pid";;
 exec) case "$3" in
  /usr/bin/true) :;;
  /usr/bin/tar) printf 'artifact bytes';;
  /guest/fail) echo 'build output'; echo 'build stderr' >&2; exit 7;;
  /guest/hang) touch "$HOME/guest-started"; sleep 120;;
  *) echo 'build output';;
 esac;;
esac
`

func TestCancelledMissingAndStalledLogReader(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "log")
	if e := syscall.Mkfifo(path, 0600); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if f, e := openLog(ctx, path); e == nil {
		f.Close()
		t.Fatal("opened without reader")
	}
	reader, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_NONBLOCK, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer syscall.Close(reader)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel2()
	writer, e := openLog(ctx2, path)
	if e != nil {
		t.Fatal(e)
	}
	defer writer.Close()
	_, e = (logWriter{ctx2, writer}).Write(make([]byte, 16<<20))
	if e == nil {
		t.Fatal("stalled reader did not cancel")
	}
}
func TestConcurrentLogLimit(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	writer := &cappedWriter{w: io.Discard, remaining: 100, cancel: cancel}
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 100; j++ {
				_, _ = writer.Write([]byte("test"))
			}
		}()
	}
	wg.Wait()
	if ctx.Err() == nil || writer.remaining != 0 {
		t.Fatal("shared limit not enforced")
	}
}
func TestParentDeathStopsCloneBeforeRecoveryLock(t *testing.T) {
	dir := t.TempDir()
	lock, e := os.OpenFile(filepath.Join(dir, "lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		t.Fatal(e)
	}
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		t.Fatal(e)
	}
	read, write, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	defer read.Close()
	defer write.Close()
	executable, e := os.Executable()
	if e != nil {
		t.Fatal(e)
	}
	marker := filepath.Join(dir, "published")
	script := filepath.Join(dir, "clone")
	if e = os.WriteFile(script, []byte("#!/bin/sh\nsleep 2\necho published > '"+marker+"'\n"), 0700); e != nil {
		t.Fatal(e)
	}
	cmd := exec.Command(executable, "--cloud-tart-child", script)
	cmd.ExtraFiles = []*os.File{read, lock}
	if e = cmd.Start(); e != nil {
		t.Fatal(e)
	}
	lock.Close()
	other, e := os.OpenFile(filepath.Join(dir, "lock"), os.O_RDWR, 0)
	if e != nil {
		t.Fatal(e)
	}
	defer other.Close()
	if syscall.Flock(int(other.Fd()), syscall.LOCK_EX|syscall.LOCK_NB) == nil {
		t.Fatal("recovery raced live helper")
	}
	write.Close() // same EOF as plugin SIGKILL
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		cmd.Process.Kill()
		t.Fatal("orphan supervisor")
	}
	if e = syscall.Flock(int(other.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		t.Fatal("lock retained after helper stopped", e)
	}
	time.Sleep(2100 * time.Millisecond)
	if _, e = os.Stat(marker); !os.IsNotExist(e) {
		t.Fatal("clone published after recovery")
	}
}
