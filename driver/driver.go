// Package driver runs operator-approved macOS qualification builds in disposable
// Tart VMs. Admission remains closed to arbitrary agents until live qualification.
package driver

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/nomad/drivers/shared/eventer"
	"github.com/hashicorp/nomad/plugins/base"
	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/hashicorp/nomad/plugins/shared/hclspec"
	pstructs "github.com/hashicorp/nomad/plugins/shared/structs"
	"github.com/shirou/gopsutil/v3/process"
)

var Version = "2.0.0-cloud.1"

type Driver struct {
	drivers.DriverSignalTaskNotSupported
	drivers.DriverExecTaskNotSupported
	config  Config
	ctx     context.Context
	eventer *eventer.Eventer
	logger  hclog.Logger
	mu      sync.Mutex
	tasks   map[string]*taskHandle
	busy    bool
	lock    *os.File
}
type taskHandle struct {
	mu     sync.Mutex
	cfg    *drivers.TaskConfig
	r      record
	ctx    context.Context
	cancel context.CancelFunc
	done   chan struct{}
	pid    int
}

func NewTartDriver(logger hclog.Logger) drivers.DriverPlugin {
	ctx := context.Background()
	return &Driver{ctx: ctx, logger: logger, eventer: eventer.NewEventer(ctx, logger), tasks: map[string]*taskHandle{}}
}
func (d *Driver) PluginInfo() (*base.PluginInfoResponse, error) {
	return &base.PluginInfoResponse{Type: base.PluginTypeDriver, PluginApiVersions: []string{drivers.ApiVersion010}, PluginVersion: Version, Name: "tart"}, nil
}
func (d *Driver) ConfigSchema() (*hclspec.Spec, error)     { return configSpec, nil }
func (d *Driver) TaskConfigSchema() (*hclspec.Spec, error) { return taskConfigSpec, nil }
func (d *Driver) Capabilities() (*drivers.Capabilities, error) {
	return &drivers.Capabilities{FSIsolation: drivers.FSIsolationImage}, nil
}

func (d *Driver) SetConfig(cfg *base.Config) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	// Configuration changes require a drained restart; never reconcile active
	// tasks merely because Nomad sends another configuration RPC.
	if d.lock != nil {
		return fmt.Errorf("driver already configured; drain and restart to change profile")
	}
	var c Config
	if e := base.MsgPackDecode(cfg.PluginConfig, &c); e != nil {
		return e
	}
	if e := c.validate(); e != nil {
		return e
	}
	if c.Enabled && (runtime.GOOS != "darwin" || runtime.GOARCH != "arm64" || os.Geteuid() == 0) {
		return fmt.Errorf("Apple Silicon and an unprivileged service UID required")
	}
	d.config = c
	if !c.Enabled {
		return nil
	}
	if e := privateDir(c.StateDir); e != nil {
		return e
	}
	for _, path := range []string{c.TartPath, filepath.Join(c.SoftnetDir, "softnet")} {
		// Resolve controller-owned current links, then require immutable ancestry.
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
				return fmt.Errorf("host executables must have root-owned immutable ancestry")
			}
		}
	}
	for _, name := range []string{"records", "vms", "tmp"} {
		p := filepath.Join(c.StateDir, name)
		if e := os.Mkdir(p, 0700); e != nil && !os.IsExist(e) {
			return e
		}
		if e := privateDir(p); e != nil {
			return e
		}
	}
	f, e := os.OpenFile(filepath.Join(c.StateDir, "driver.lock"), os.O_CREATE|os.O_RDWR|syscall.O_NOFOLLOW, 0600)
	if e != nil {
		return e
	}
	if e = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); e != nil {
		f.Close()
		return fmt.Errorf("another driver owns this VM store")
	}
	d.lock = f
	if e = d.reconcile(); e != nil {
		d.lock.Close()
		d.lock = nil
		return e
	}
	return nil
}
func (d *Driver) reconcile() error {
	entries, e := os.ReadDir(filepath.Join(d.config.StateDir, "records"))
	if e != nil {
		return e
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".record-") {
			continue
		} // incomplete atomic write; not authority
		if !strings.HasSuffix(entry.Name(), ".json") {
			return fmt.Errorf("unknown ownership entry")
		}
		r, e := readRecord(filepath.Join(d.config.StateDir, "records", entry.Name()))
		if e != nil {
			return e
		}
		if r.CleanupPending {
			if e = d.cleanup(&r); e != nil {
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
func (d *Driver) Fingerprint(ctx context.Context) (<-chan *drivers.Fingerprint, error) {
	ch := make(chan *drivers.Fingerprint)
	go func() {
		defer close(ch)
		for {
			fp := &drivers.Fingerprint{Health: drivers.HealthStateUndetected, HealthDescription: "disabled", Attributes: map[string]*pstructs.Attribute{}}
			if d.config.Enabled {
				fp.Health = drivers.HealthStateHealthy
				fp.HealthDescription = "trusted qualification only"
				probe, cancel := context.WithTimeout(ctx, 5*time.Second)
				_, e := d.small(probe, "--version")
				cancel()
				d.mu.Lock()
				blocked := d.busy
				d.mu.Unlock()
				if e != nil {
					fp.Health = drivers.HealthStateUnhealthy
					fp.HealthDescription = "Tart probe failed"
				}
				fp.Attributes["driver.tart"] = pstructs.NewBoolAttribute(true)
				fp.Attributes["driver.tart.version"] = pstructs.NewStringAttribute(Version)
				fp.Attributes["driver.tart.image"] = pstructs.NewStringAttribute(d.config.Image)
				fp.Attributes["driver.tart.xcode"] = pstructs.NewStringAttribute(d.config.Xcode)
				fp.Attributes["driver.tart.vcpus"] = pstructs.NewIntAttribute(int64(d.config.VCPUs), "")
				fp.Attributes["driver.tart.memory_mb"] = pstructs.NewIntAttribute(d.config.MemoryMB, "MiB")
				fp.Attributes["driver.tart.qualification_only"] = pstructs.NewBoolAttribute(true)
				fp.Attributes["driver.tart.busy"] = pstructs.NewBoolAttribute(blocked)
			}
			select {
			case ch <- fp:
			case <-ctx.Done():
				return
			}
			select {
			case <-time.After(10 * time.Second):
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch, nil
}
func (d *Driver) StartTask(cfg *drivers.TaskConfig) (*drivers.TaskHandle, *drivers.DriverNetwork, error) {
	var tc TaskConfig
	if e := cfg.DecodeDriverConfig(&tc); e != nil {
		return nil, nil, e
	}
	if e := d.config.validateTask(cfg, tc); e != nil {
		return nil, nil, e
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.busy {
		return nil, nil, fmt.Errorf("VM store busy or cleanup pending")
	}
	if _, ok := d.tasks[cfg.ID]; ok {
		return nil, nil, fmt.Errorf("task already exists")
	}
	// Existing task records must be recovered/destroyed, never overwritten by a
	// new StartTask RPC after a lost response.
	if _, e := os.Lstat(d.recordPath(cfg.ID)); !os.IsNotExist(e) {
		return nil, nil, fmt.Errorf("existing task must be recovered")
	}
	r := newRecord(cfg.ID, cfg.AllocID, d.config.Image)
	if e := d.save(r); e != nil {
		return nil, nil, e
	}
	handle := drivers.NewTaskHandle(2)
	handle.Config = cfg
	handle.State = drivers.TaskStateRunning
	if e := handle.SetDriverState(r); e != nil {
		return nil, nil, e
	}
	ctx, cancel := context.WithTimeout(d.ctx, time.Duration(d.config.TimeoutSeconds)*time.Second)
	h := &taskHandle{cfg: cfg, r: r, ctx: ctx, cancel: cancel, done: make(chan struct{})}
	d.tasks[cfg.ID] = h
	d.busy = true
	go d.runTask(h, tc)
	return handle, nil, nil
}
func (d *Driver) RecoverTask(handle *drivers.TaskHandle) error {
	if handle == nil || handle.Config == nil || handle.Version != 2 {
		return fmt.Errorf("unsupported task handle")
	}
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, ok := d.tasks[handle.Config.ID]; ok {
		return nil
	}
	r, e := readRecord(d.recordPath(handle.Config.ID))
	if e != nil {
		return e
	}
	if r.CleanupPending {
		return fmt.Errorf("cleanup required before recovery")
	}
	h := &taskHandle{cfg: handle.Config, r: r, done: make(chan struct{}), cancel: func() {}}
	close(h.done)
	d.tasks[r.TaskID] = h
	return nil
}
func (d *Driver) task(id string) (*taskHandle, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	h, ok := d.tasks[id]
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}
	return h, nil
}
func result(r record) *drivers.ExitResult {
	out := &drivers.ExitResult{ExitCode: r.ExitCode}
	if r.Failure != "" {
		out.Err = fmt.Errorf("%s", r.Failure)
	}
	return out
}
func (d *Driver) WaitTask(ctx context.Context, id string) (<-chan *drivers.ExitResult, error) {
	h, e := d.task(id)
	if e != nil {
		return nil, e
	}
	ch := make(chan *drivers.ExitResult, 1)
	go func() {
		defer close(ch)
		select {
		case <-ctx.Done():
			return
		case <-h.done:
			h.mu.Lock()
			r := h.r
			h.mu.Unlock()
			ch <- result(r)
		}
	}()
	return ch, nil
}
func (d *Driver) StopTask(id string, timeout time.Duration, signal string) error {
	h, e := d.task(id)
	if e != nil {
		return e
	}
	h.cancel()
	select {
	case <-h.done:
	case <-time.After(90 * time.Second):
		return fmt.Errorf("cancellation cleanup deadline exceeded")
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if h.r.CleanupPending {
		return fmt.Errorf("cleanup pending")
	}
	return nil
}
func (d *Driver) DestroyTask(id string, force bool) error {
	h, e := d.task(id)
	if e != nil {
		return e
	}
	select {
	case <-h.done:
	default:
		if !force {
			return fmt.Errorf("task is running")
		}
		if e = d.StopTask(id, 0, ""); e != nil {
			return e
		}
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	pending := h.r.CleanupPending
	if h.r.CleanupPending {
		if e = d.cleanup(&h.r); e != nil {
			return e
		}
		if e = d.save(h.r); e != nil {
			return e
		}
	}
	if e = os.Remove(d.recordPath(id)); e != nil {
		return e
	}
	d.mu.Lock()
	delete(d.tasks, id)
	if pending {
		d.busy = false
	}
	d.mu.Unlock()
	return nil
}
func (d *Driver) InspectTask(id string) (*drivers.TaskStatus, error) {
	h, e := d.task(id)
	if e != nil {
		return nil, e
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	r := h.r
	state := drivers.TaskStateRunning
	if !r.Finished.IsZero() {
		state = drivers.TaskStateExited
	}
	return &drivers.TaskStatus{ID: id, Name: h.cfg.Name, State: state, StartedAt: r.Started, CompletedAt: r.Finished, ExitResult: result(r), DriverAttributes: map[string]string{"phase": r.Phase, "vm": r.VM, "image": r.Image, "artifact_sha256": r.ArtifactSHA256}}, nil
}
func (d *Driver) event(h *taskHandle, phase string) {
	h.mu.Lock()
	r := h.r
	h.mu.Unlock()
	d.eventer.EmitEvent(&drivers.TaskEvent{TaskID: r.TaskID, TaskName: h.cfg.Name, AllocID: r.AllocID, Timestamp: time.Now(), Message: "Tart build: " + phase, Annotations: map[string]string{"tart.phase": phase, "tart.vm": r.VM, "tart.image": r.Image, "tart.artifact_sha256": r.ArtifactSHA256}})
}
func (d *Driver) TaskEvents(ctx context.Context) (<-chan *drivers.TaskEvent, error) {
	return d.eventer.TaskEvents(ctx)
}

// Statistics are host Tart-process readings, not guest-used RAM. Virtualization
// helper attribution remains a live qualification item; publish only measured fields.
func (d *Driver) TaskStats(ctx context.Context, id string, interval time.Duration) (<-chan *drivers.TaskResourceUsage, error) {
	h, e := d.task(id)
	if e != nil {
		return nil, e
	}
	ch := make(chan *drivers.TaskResourceUsage)
	if interval < time.Second {
		interval = time.Second
	}
	go func() {
		defer close(ch)
		var last float64
		var previous time.Time
		for {
			h.mu.Lock()
			pid := h.pid
			h.mu.Unlock()
			if pid > 0 {
				p, e := process.NewProcess(int32(pid))
				if e == nil {
					m, me := p.MemoryInfoWithContext(ctx)
					t, te := p.TimesWithContext(ctx)
					now := time.Now()
					if me == nil && te == nil {
						cpu := &drivers.CpuStats{}
						if !previous.IsZero() {
							cpu.Percent = 100 * (t.Total() - last) / now.Sub(previous).Seconds()
							cpu.Measured = []string{"Percent"}
						}
						last = t.Total()
						previous = now
						sample := &drivers.TaskResourceUsage{Timestamp: now.UnixNano(), ResourceUsage: &drivers.ResourceUsage{CpuStats: cpu, MemoryStats: &drivers.MemoryStats{RSS: m.RSS, Swap: m.Swap, Measured: []string{"RSS", "Swap"}}}}
						select {
						case ch <- sample:
						case <-ctx.Done():
							return
						case <-h.done:
							return
						}
					}
				}
			}
			select {
			case <-ctx.Done():
				return
			case <-h.done:
				return
			case <-time.After(interval):
			}
		}
	}()
	return ch, nil
}
