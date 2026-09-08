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
	plugin "github.com/hashicorp/go-plugin"
	"github.com/hashicorp/nomad/client/lib/cpustats"
	"github.com/hashicorp/nomad/client/lib/numalib"
	"github.com/hashicorp/nomad/drivers/shared/executor"
	"github.com/hashicorp/nomad/helper/pluginutils/hclutils"
	"github.com/hashicorp/nomad/nomad/structs"
	"github.com/hashicorp/nomad/plugins/base"
	"github.com/hashicorp/nomad/plugins/drivers"
)

func buildFixture(t *testing.T) (*Driver, *drivers.TaskConfig) {
	t.Helper()
	root, e := filepath.EvalSymlinks(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	c := BuildConfig{TartPath: filepath.Join(root, "tart"), SoftnetDir: root, StateDir: root, Image: "ghcr.io/test/image@sha256:" + strings.Repeat("a", 64), Xcode: "26.5", CPU: 20000, VCPUs: 4, MemoryMB: 8192, OverheadMB: 1024, TimeoutSeconds: 30, ArtifactMB: 1, ImageDiskMB: 1, MinFreeDiskMB: 1024, Jobs: []string{"qualify"}, Block: []string{"0.0.0.0/0", "@host"}}
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
	d.config = &Config{Enabled: true, Build: &c}
	lock, e := os.OpenFile(filepath.Join(root, "driver.lock"), os.O_CREATE|os.O_RDWR, 0600)
	if e != nil {
		t.Fatal(e)
	}
	if e = syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		t.Fatal(e)
	}
	lr, lw, e := os.Pipe()
	if e != nil {
		t.Fatal(e)
	}
	d.build = &buildStore{lock: lock, lifeReader: lr, lifeWriter: lw}
	d.client = &tartCLI{logger: hclog.NewNullLogger(), runner: fastTestRunner{buildRunner{&c, d.build}}}
	d.executorFactory = func(*drivers.TaskConfig, *drivers.TaskHandle) (executor.Executor, *plugin.Client, error) {
		return executor.NewExecutor(hclog.NewNullLogger(), cpustats.Compute{}), nil, nil
	}
	t.Cleanup(func() {
		d.tasks.lock.RLock()
		var tasks []*taskHandle
		for _, h := range d.tasks.store {
			tasks = append(tasks, h)
		}
		d.tasks.lock.RUnlock()
		for _, h := range tasks {
			if h.shutdown != nil {
				h.shutdown()
			}
			select {
			case <-h.doneCh:
			case <-time.After(90 * time.Second):
				t.Error("test left an active handle")
				return
			}
		}
		lw.Close()
		lr.Close()
		lock.Close()
	})
	cfg := &drivers.TaskConfig{ID: "task1", AllocID: "alloc1", JobName: "qualify", JobID: "qualify", Namespace: "canary", Name: "build", AllocDir: filepath.Join(root, "alloc"), StdoutPath: out, StderrPath: errout, Resources: &drivers.Resources{NomadResources: &structs.AllocatedTaskResources{Cpu: structs.AllocatedCpuResources{CpuShares: 20000}, Memory: structs.AllocatedMemoryResources{MemoryMB: 9216}}}}
	return d, cfg
}
func startBuildTest(t *testing.T, d *Driver, cfg *drivers.TaskConfig, command string) *drivers.TaskHandle {
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
func waitBuildTest(t *testing.T, d *Driver, id string) *drivers.ExitResult {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
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
	d, cfg := buildFixture(t)
	cfg.Env = map[string]string{"TART_HOME": "/unowned", "DYLD_INSERT_LIBRARIES": "/evil", "PATH": "/evil", "NOMAD_TOKEN": "secret"}
	startBuildTest(t, d, cfg, "/guest/fail")
	r := waitBuildTest(t, d, cfg.ID)
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
	hostenv, _ := os.ReadFile(filepath.Join(d.config.Build.StateDir, "environment"))
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
	d, cfg := buildFixture(t)
	if e := d.config.Build.validate(); e != nil {
		t.Fatal(e)
	}
	for _, mutate := range []func(*drivers.TaskConfig){func(c *drivers.TaskConfig) { c.User = "root" }, func(c *drivers.TaskConfig) { c.Namespace = "default" }, func(c *drivers.TaskConfig) { c.JobID = "untrusted" }, func(c *drivers.TaskConfig) { c.Resources.NomadResources.Cpu.CpuShares = 100 }, func(c *drivers.TaskConfig) { c.Resources.NomadResources.Memory.MemoryMB = 8192 }} {
		copy := cfg.Copy()
		mutate(copy)
		if e := d.config.Build.validateTask(copy, TaskConfig{Command: "true"}); e == nil {
			t.Fatal("unsafe request accepted")
		}
	}
	d.admission.Lock()
	d.build.busy = true
	d.admission.Unlock()
	cfg.EncodeConcreteDriverConfig(TaskConfig{Command: "true"})
	if _, _, e := d.StartTask(cfg); e == nil {
		t.Fatal("concurrent admission accepted")
	}
}
func TestCleanupFailureRemainsRecoverable(t *testing.T) {
	d, cfg := buildFixture(t)
	os.WriteFile(filepath.Join(d.config.Build.StateDir, "fail-delete"), nil, 0600)
	startBuildTest(t, d, cfg, "/guest/ok")
	r := waitBuildTest(t, d, cfg.ID)
	if r.Err == nil {
		t.Fatal("cleanup failure reported success")
	}
	if e := d.DestroyTask(cfg.ID, false); e == nil {
		t.Fatal("forgot failed cleanup")
	}
	if _, e := os.Stat(d.recordPath(cfg.ID)); e != nil {
		t.Fatal("ownership lost")
	}
	os.Remove(filepath.Join(d.config.Build.StateDir, "fail-delete"))
	restarted := NewTartDriver(hclog.NewNullLogger()).(*Driver)
	restarted.config = d.config
	restarted.build = d.build
	restarted.client = d.client
	if e := restarted.reconcileBuilds(); e != nil {
		t.Fatal(e)
	}
	r2, e := readRecord(d.recordPath(cfg.ID))
	if e != nil || r2.CleanupPending {
		t.Fatalf("reconcile failed: %+v %v", r2, e)
	}
	calls, _ := os.ReadFile(filepath.Join(d.config.Build.StateDir, "calls"))
	if strings.Count(string(calls), "clone\n") != 1 {
		t.Fatal("recovery replayed clone")
	}
}
func TestRecoveryDoesNotReplayAndUnknownRecordFailsClosed(t *testing.T) {
	d, cfg := buildFixture(t)
	r := newRecord(cfg.ID, cfg.AllocID, d.config.Build.Image)
	if e := d.save(r); e != nil {
		t.Fatal(e)
	}
	if e := d.reconcileBuilds(); e != nil {
		t.Fatal(e)
	}
	h := drivers.NewTaskHandle(taskHandleVersion)
	h.Config = cfg
	h.SetDriverState(&driverState{TaskConfig: cfg, Build: true})
	if e := d.RecoverTask(h); e != nil {
		t.Fatal(e)
	}
	result := waitBuildTest(t, d, cfg.ID)
	if result.Err == nil || result.ExitCode != -1 {
		t.Fatalf("interrupted build became success: %+v", result)
	}
	r.VM = "user-vm"
	b, _ := json.Marshal(r)
	os.WriteFile(d.recordPath(cfg.ID), b, 0600)
	if e := d.reconcileBuilds(); e == nil {
		t.Fatal("foreign VM record accepted")
	}
}
func TestCancellationStopsBackgroundVM(t *testing.T) {
	d, cfg := buildFixture(t)
	startBuildTest(t, d, cfg, "/guest/hang")
	deadline := time.Now().Add(4 * time.Second)
	for time.Now().Before(deadline) {
		if _, e := os.Stat(filepath.Join(d.config.Build.StateDir, "guest-started")); e == nil {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if e := d.StopTask(cfg.ID, time.Second, "SIGTERM"); e != nil {
		t.Fatal(e)
	}
	r := waitBuildTest(t, d, cfg.ID)
	if r.Err == nil {
		t.Fatal("cancelled build succeeded")
	}
	if _, e := os.Stat(filepath.Join(d.config.Build.StateDir, "vm-name")); !os.IsNotExist(e) {
		t.Fatal("VM survived cancellation")
	}
}
func TestRegularSourceAndArtifactLimits(t *testing.T) {
	d, _ := buildFixture(t)
	path := filepath.Join(d.config.Build.StateDir, "link")
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
 ip) echo '192.0.2.1';;
 clone) echo "$3" > "$HOME/vm-name"; echo stopped > "$HOME/vm-state"; env > "$HOME/environment";;
 set) :;;
 list) if test -f "$HOME/vm-name"; then printf '[{"Name":"%s","Source":"local","State":"%s"}]' "$(cat "$HOME/vm-name")" "$(cat "$HOME/vm-state")"; else echo '[]'; fi;;
 run) echo running > "$HOME/vm-state"; echo $$ > "$HOME/vm-pid"; trap 'echo stopped > "$HOME/vm-state"; exit 0' TERM INT; while :; do sleep 0.1; done;;
 stop) test -f "$HOME/vm-name"; if test -f "$HOME/vm-pid"; then kill -TERM "$(cat "$HOME/vm-pid")" 2>/dev/null || true; fi; echo stopped > "$HOME/vm-state";;
 delete) test -f "$HOME/vm-name"; test ! -f "$HOME/fail-delete"; rm -f "$HOME/vm-name" "$HOME/vm-state" "$HOME/vm-pid";;
 exec) case "$3" in
  /usr/bin/true) test ! -f "$HOME/not-ready";;
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

func TestBuildProfilePreservesOrdinaryConfiguration(t *testing.T) {
	var cfg Config
	hclutils.NewConfigParser(configSpec).ParseHCL(t, `config { enabled = true }`, &cfg)
	if !cfg.Enabled || cfg.Build != nil {
		t.Fatal("ordinary config changed")
	}
	var tc TaskConfig
	hclutils.NewConfigParser(taskConfigSpec).ParseHCL(t, `config { url = "ghcr.io/example/image:latest" ssh_user = "admin" ssh_password = "pass" directory { name = "project" path = "/tmp/project" } network { mode = "bridged" bridged_interface = "en0" } }`, &tc)
	d := NewTartDriver(hclog.NewNullLogger()).(*Driver)
	d.config = &cfg
	if e := d.validateTask(&drivers.TaskConfig{}, tc); e != nil {
		t.Fatal(e)
	}
	if tc.Network.Mode != "bridged" || tc.Directories[0].Path != "/tmp/project" {
		t.Fatal("ordinary options lost")
	}
}
func TestBuildProfileHCLAndOverrideDenials(t *testing.T) {
	d, cfg := buildFixture(t)
	raw := `config { enabled=true build { tart_path="/opt/tart" softnet_dir="/opt" state_dir="/private/var/lib/build" image="ghcr.io/test/image@sha256:` + strings.Repeat("a", 64) + `" xcode="26.5" cpu_mhz=20000 vcpus=4 memory_mb=8192 overhead_mb=1024 timeout_seconds=30 artifact_mb=1 image_disk_mb=133515 min_free_disk_mb=32768 network_allow=[] network_block=["0.0.0.0/0","@host"] qualification_jobs=["qualify"] } }`
	var decoded Config
	hclutils.NewConfigParser(configSpec).ParseHCL(t, raw, &decoded)
	if decoded.Build == nil {
		t.Fatal("build block ignored")
	}
	if e := decoded.Build.validate(); e != nil {
		t.Fatal(e)
	}
	for _, change := range []func(*TaskConfig){
		func(c *TaskConfig) { c.URL = "ghcr.io/evil/image" }, func(c *TaskConfig) { c.SSHUser = "admin" }, func(c *TaskConfig) { c.SSHPassword = "secret" }, func(c *TaskConfig) { c.Auth.Password = "secret" }, func(c *TaskConfig) { c.ShowUI = true }, func(c *TaskConfig) { c.DiskSize = 100 }, func(c *TaskConfig) { c.PullOnly = true }, func(c *TaskConfig) { c.Network = &NetworkConfig{Mode: "host"} }, func(c *TaskConfig) { c.RootDisk = &RootDiskOptions{} }, func(c *TaskConfig) { c.Directories = []DirectoryMount{{Path: "/"}} },
	} {
		tc := TaskConfig{Command: "true"}
		change(&tc)
		cfg.EncodeConcreteDriverConfig(tc)
		if _, _, e := d.StartTask(cfg); e == nil {
			t.Fatal("forbidden override accepted", tc)
		}
	}
	entries, e := os.ReadDir(filepath.Join(d.config.Build.StateDir, "records"))
	if e != nil || len(entries) != 0 {
		t.Fatal("denial mutated VM records", e)
	}
	if _, e = os.Stat(filepath.Join(d.config.Build.StateDir, "calls")); !os.IsNotExist(e) {
		t.Fatal("denial reached Tart")
	}
}
func TestBuildArgsNeverMountSecretsOrUseJobNetworking(t *testing.T) {
	d, cfg := buildFixture(t)
	vm := VMConfig{Nomad: cfg, Name: "owned", Build: d.config.Build, Driver: TaskConfig{GuestAgent: true}}
	args, e := d.client.BuildStartArgs(vm)
	if e != nil {
		t.Fatal(e)
	}
	text := strings.Join(args, " ")
	for _, required := range []string{"--no-clipboard", "--no-audio", "--no-graphics", "--net-softnet-block 0.0.0.0/0,@host"} {
		if !strings.Contains(text, required) {
			t.Fatal("missing restriction", text)
		}
	}
	for _, forbidden := range []string{"--dir", "--net-host", "--net-bridged", "--net-softnet-expose"} {
		if strings.Contains(text, forbidden) {
			t.Fatal("host exposure", text)
		}
	}
}
func TestRecoverOldHandleNeverReplaysTask(t *testing.T) {
	d := NewTartDriver(hclog.NewNullLogger()).(*Driver)
	h := drivers.NewTaskHandle(1)
	h.Config = &drivers.TaskConfig{ID: "old"}
	h.SetDriverState(&driverState{TaskConfig: h.Config})
	if e := d.RecoverTask(h); e == nil {
		t.Fatal("old handle replayed")
	}
	if _, ok := d.tasks.Get("old"); ok {
		t.Fatal("recovery launched a replacement")
	}
}
func TestDestroyFailureRetainsOrdinaryHandle(t *testing.T) {
	d := NewTartDriver(hclog.NewNullLogger()).(*Driver)
	mock := &testClient{}
	mock.deleteErr = io.ErrUnexpectedEOF
	d.client = mock
	d.tasks.Set("t", &taskHandle{taskConfig: &drivers.TaskConfig{ID: "t", AllocID: "a"}, state: drivers.TaskStateExited})
	if e := d.DestroyTask("t", false); e == nil {
		t.Fatal("cleanup failure swallowed")
	}
	if _, ok := d.tasks.Get("t"); !ok {
		t.Fatal("forgot ownership")
	}
}

type fastTestRunner struct{ runner }

func (r fastTestRunner) Run(ctx context.Context, name string, args ...string) *exec.Cmd {
	cmd := r.runner.Run(ctx, name, args...)
	cmd.Env = append(cmd.Env, "GORACE=atexit_sleep_ms=0")
	return cmd
}

func TestOrdinaryRecoveryReattachesExecutorWithoutReplay(t *testing.T) {
	d, cfg := buildFixture(t)
	// Run the normal path with a real Nomad executor subprocess/RPC connection.
	profile := d.config.Build
	d.config = &Config{Enabled: true}
	d.executorFactory = nil
	d.nomadConfig = &base.ClientDriverConfig{Topology: &numalib.Topology{}}
	t.Setenv("PATH", profile.StateDir+":"+os.Getenv("PATH"))
	// The ordinary backend deliberately retains the operator's environment.
	t.Setenv("HOME", profile.StateDir)
	t.Setenv("GORACE", "atexit_sleep_ms=0")
	cfg.Env = map[string]string{"HOME": profile.StateDir}
	cfg.EncodeConcreteDriverConfig(TaskConfig{URL: profile.Image, GuestAgent: true, Command: "/guest/ok"})
	h, _, e := d.StartTask(cfg)
	if e != nil {
		t.Fatal(e)
	}
	original, _ := d.tasks.Get(cfg.ID)
	t.Cleanup(func() {
		if original.pluginClient != nil {
			original.pluginClient.Kill()
		}
	})
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		b, _ := os.ReadFile(cfg.StdoutPath)
		if strings.Contains(string(b), "build output") {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	d2 := NewTartDriver(hclog.NewNullLogger()).(*Driver)
	d2.config = d.config
	d2.client = d.client
	if e = d2.RecoverTask(h); e != nil {
		t.Fatal(e)
	}
	if e = d2.StopTask(cfg.ID, time.Second, "SIGINT"); e != nil {
		t.Fatal(e)
	}
	waitBuildTest(t, d2, cfg.ID)
	if e = d2.StopTask(cfg.ID, time.Second, "SIGINT"); e != nil {
		t.Fatal("repeated stop", e)
	}
	if e = d2.DestroyTask(cfg.ID, false); e != nil {
		t.Fatal("destroy after stop", e)
	}
	if _, exists := d2.tasks.Get(cfg.ID); exists {
		t.Fatal("destroy retained handle")
	}
	select {
	case <-original.doneCh:
	case <-time.After(5 * time.Second):
		t.Fatal("old observer not released")
	}
	calls, _ := os.ReadFile(filepath.Join(profile.StateDir, "calls"))
	if strings.Count(string(calls), "clone\n") != 1 {
		t.Fatal("recovery replayed setup", string(calls))
	}
}

func TestBuildProfileRequiresFreshDriver(t *testing.T) {
	d := NewTartDriver(hclog.NewNullLogger()).(*Driver)
	var config []byte
	if e := base.MsgPackEncode(&config, Config{Enabled: true}); e != nil {
		t.Fatal(e)
	}
	if e := d.SetConfig(&base.Config{PluginConfig: config}); e != nil {
		t.Fatal(e)
	}
	if e := base.MsgPackEncode(&config, Config{Enabled: true, Build: &BuildConfig{}}); e != nil {
		t.Fatal(e)
	}
	if e := d.SetConfig(&base.Config{PluginConfig: config}); e == nil || !strings.Contains(e.Error(), "drain and restart") {
		t.Fatal("mode changed in place", e)
	}
}
func TestGuestReadinessRequiresSuccessfulExit(t *testing.T) {
	d, cfg := buildFixture(t)
	if e := os.WriteFile(filepath.Join(d.config.Build.StateDir, "not-ready"), nil, 0600); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if e := d.waitForGuestAgent(ctx, VMConfig{Nomad: cfg, Name: "test", Driver: TaskConfig{GuestAgent: true}}); e == nil {
		t.Fatal("nonzero readiness exit accepted")
	}
}
func TestStopRecoveredBuildIsIdempotent(t *testing.T) {
	d, cfg := buildFixture(t)
	handle := startBuildTest(t, d, cfg, "/guest/ok")
	if r := waitBuildTest(t, d, cfg.ID); r.Err != nil {
		t.Fatal(r.Err)
	}
	d.tasks.Delete(cfg.ID)
	if e := d.RecoverTask(handle); e != nil {
		t.Fatal(e)
	}
	if e := d.StopTask(cfg.ID, time.Second, ""); e != nil {
		t.Fatal(e)
	}
}

func TestBuildRejectsSoftnetBridgeIsolationOverride(t *testing.T) {
	d, _ := buildFixture(t)
	for _, allow := range []string{"0.0.0.0/0", "192.168.1.1/0"} {
		d.config.Build.Allow = []string{allow}
		if e := d.config.Build.validate(); e == nil || !strings.Contains(e.Error(), "bridge isolation") {
			t.Fatalf("accepted %q: %v", allow, e)
		}
	}
}
