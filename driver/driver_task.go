package driver

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	plugin "github.com/hashicorp/go-plugin"
	"github.com/hashicorp/nomad/client/lib/cpustats"
	"github.com/hashicorp/nomad/drivers/shared/executor"
	"github.com/hashicorp/nomad/plugins/drivers"
	pstructs "github.com/hashicorp/nomad/plugins/shared/structs"
)

func (d *Driver) createExecutor(cfg *drivers.TaskConfig, handle *drivers.TaskHandle) (executor.Executor, *plugin.Client, error) {
	if d.executorFactory != nil {
		return d.executorFactory(cfg, handle)
	}
	if d.nomadConfig == nil || d.nomadConfig.Topology == nil {
		return nil, nil, fmt.Errorf("Nomad executor topology unavailable")
	}
	pluginLogFile := filepath.Join(cfg.TaskDir().Dir, "executor.out")
	execConfig := &executor.ExecutorConfig{
		LogFile:  pluginLogFile,
		LogLevel: "debug",
	}

	logger := d.logger.With("task_name", handle.Config.Name, "alloc_id", handle.Config.AllocID)
	execImpl, pluginClient, err := executor.CreateExecutor(logger, d.nomadConfig, execConfig)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create executor: %w", err)
	}
	return execImpl, pluginClient, nil
}

func openTaskLog(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
}

func (d *Driver) resolveDriverNetwork(vmConfig VMConfig) (*drivers.DriverNetwork, error) {
	ctx, cancel := context.WithTimeout(d.ctx, 45*time.Second)
	defer cancel()

	ip, err := d.waitForIPAddress(ctx, vmConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to determine VM IP for driver network override: %w", err)
	}

	return &drivers.DriverNetwork{IP: ip}, nil
}

func (d *Driver) emitTaskEvent(cfg *drivers.TaskConfig, msg string, annotations map[string]string) {
	d.eventer.EmitEvent(&drivers.TaskEvent{
		TaskID:      cfg.ID,
		TaskName:    cfg.Name,
		AllocID:     cfg.AllocID,
		Timestamp:   time.Now(),
		Message:     msg,
		Annotations: annotations,
	})
}

// startPullOnlyTask runs `tart pull <url>` via the executor so that the image
// is cached locally on the Nomad client. No VM is created; the task
// completes as soon as the pull exits.
func (d *Driver) startPullOnlyTask(cfg *drivers.TaskConfig, vmConfig VMConfig, handle *drivers.TaskHandle) (*drivers.TaskHandle, *drivers.DriverNetwork, error) {
	d.logger.Info("starting tart pull-only task", "url", vmConfig.Driver.URL)

	if _, err := d.client.PrepareRegistryEnv(d.ctx, vmConfig); err != nil {
		return nil, nil, fmt.Errorf("failed to prepare registry env: %w", err)
	}

	d.emitTaskEvent(cfg, "Pulling VM image", map[string]string{
		"url": vmConfig.Driver.URL,
	})

	execImpl, pluginClient, err := d.createExecutor(cfg, handle)
	if err != nil {
		return nil, nil, err
	}

	path, err := d.tartPath()
	if err != nil {
		pluginClient.Kill()
		return nil, nil, err
	}
	execCmd := &executor.ExecCommand{
		Cmd:              path,
		Args:             d.client.BuildPullArgs(vmConfig),
		Env:              tartEnvList(cfg),
		User:             cfg.User,
		TaskDir:          cfg.TaskDir().Dir,
		StdoutPath:       cfg.StdoutPath,
		StderrPath:       cfg.StderrPath,
		NetworkIsolation: cfg.NetworkIsolation,
	}

	ps, err := execImpl.Launch(execCmd)
	if err != nil {
		pluginClient.Kill()
		return nil, nil, fmt.Errorf("failed to launch pull: %w", err)
	}

	state := driverState{
		TaskConfig:     cfg,
		StartedAt:      time.Now(),
		PullOnly:       true,
		Pid:            ps.Pid,
		ReattachConfig: pstructs.ReattachConfigFromGoPlugin(pluginClient.ReattachConfig()),
	}
	handle.State = drivers.TaskStateRunning
	if err := handle.SetDriverState(&state); err != nil {
		execImpl.Shutdown("", 0)
		pluginClient.Kill()
		return nil, nil, fmt.Errorf("failed to set driver state: %w", err)
	}

	h := &taskHandle{
		exec:         execImpl,
		pluginClient: pluginClient,
		pid:          ps.Pid,
		taskConfig:   cfg,
		state:        drivers.TaskStateRunning,
		startedAt:    time.Now(),
		logger:       d.logger,
		doneCh:       make(chan struct{}),
		pullOnly:     true,
	}

	d.tasks.Set(cfg.ID, h)
	go h.run()

	return handle, nil, nil
}

// StartTask validates policy before any registry, mount, VM or executor operation.
func (d *Driver) StartTask(cfg *drivers.TaskConfig) (*drivers.TaskHandle, *drivers.DriverNetwork, error) {
	d.admission.Lock()
	defer d.admission.Unlock()
	if _, ok := d.tasks.Get(cfg.ID); ok {
		return nil, nil, fmt.Errorf("task already started")
	}
	var tc TaskConfig
	if err := cfg.DecodeDriverConfig(&tc); err != nil {
		return nil, nil, err
	}
	if err := d.validateTask(cfg, tc); err != nil {
		return nil, nil, err
	}
	handle := drivers.NewTaskHandle(taskHandleVersion)
	handle.Config = cfg
	vm := VMConfig{Driver: tc, Nomad: cfg, Name: vmName(cfg.AllocID + "-" + taskKey(cfg.ID)[:12]), Build: d.config.Build}
	if tc.PullOnly {
		return d.startPullOnlyTask(cfg, vm, handle)
	}
	h := &taskHandle{taskConfig: cfg, vmConfig: vm, state: drivers.TaskStateRunning, startedAt: time.Now(), logger: d.logger, doneCh: make(chan struct{})}
	if vm.Build != nil {
		if d.build == nil || d.build.busy {
			return nil, nil, fmt.Errorf("build store unavailable, busy or cleanup pending")
		}
		if _, e := os.Lstat(d.recordPath(cfg.ID)); !os.IsNotExist(e) {
			return nil, nil, fmt.Errorf("existing task must be recovered")
		}
		r := newRecord(cfg.ID, cfg.AllocID, vm.Build.Image)
		if e := d.save(r); e != nil {
			return nil, nil, e
		}
		h.build = &r
		vm.Name = r.VM
		vm.Driver.URL = r.Image
		vm.Driver.GuestAgent = true
		h.vmConfig = vm
		ctx, cancel := context.WithTimeout(d.ctx, time.Duration(vm.Build.TimeoutSeconds)*time.Second)
		h.startupCancel = cancel
		h.shutdown = cancel
		diskDone := make(chan struct{})
		h.launch = func() error {
			go func() {
				defer close(diskDone)
				if e := monitorDisk(ctx, time.Second, uint64(vm.Build.MinFreeDiskMB)*1024*1024, func() (uint64, error) { return freeDisk(vm.Build.StateDir) }); e != nil {
					h.failBuild(e)
					cancel()
				}
			}()
			return d.launchTask(ctx, h, handle)
		}
		h.finish = func(result *drivers.ExitResult) *drivers.ExitResult {
			<-diskDone
			return d.finishBuild(h, result)
		}
		if e := handle.SetDriverState(&driverState{TaskConfig: cfg, StartedAt: h.startedAt, VMName: vm.Name, Build: true}); e != nil {
			return nil, nil, e
		}
		handle.State = drivers.TaskStateRunning
		d.build.busy = true
		d.tasks.Set(cfg.ID, h)
		go h.run()
		return handle, nil, nil
	}
	// Keep the existing synchronous network override for ordinary VM tasks.
	if e := d.launchTask(d.ctx, h, handle); e != nil {
		return nil, nil, e
	}
	d.tasks.Set(cfg.ID, h)
	go h.run()
	return handle, h.networkOverride.Copy(), nil
}

// launchTask is shared by SSH VMs and disposable guest-agent builds.
func (d *Driver) launchTask(ctx context.Context, h *taskHandle, handle *drivers.TaskHandle) (err error) {
	cfg, vm := h.taskConfig, h.vmConfig
	if vm.Build == nil {
		vm.Driver.Directories = resolveDirectoryMounts(cfg, vm.Driver.Directories)
		h.vmConfig = vm
	}
	needsDownload, e := d.client.NeedsImageDownload(ctx, vm)
	if e != nil {
		return e
	}
	if vm.Build != nil {
		available, e := freeDisk(vm.Build.StateDir)
		if e != nil {
			return e
		}
		if e = vm.Build.checkDisk(available, needsDownload); e != nil {
			return e
		}
	}
	if needsDownload {
		d.emitTaskEvent(cfg, "Downloading VM image", map[string]string{"url": vm.Driver.URL})
	}
	// Even partial Setup may have created the destination. Build finalization
	// journals failed cleanup; ordinary startup also attempts owned cleanup.
	defer func() {
		if err != nil && h.build == nil {
			if cleanupErr := d.cleanupVM(vm.Name); cleanupErr != nil {
				err = errors.Join(err, fmt.Errorf("startup cleanup failed: %w", cleanupErr))
			}
		}
	}()
	if _, e = d.client.Setup(ctx, vm); e != nil {
		return e
	}
	if needsDownload {
		d.emitTaskEvent(cfg, "VM image download complete", map[string]string{"url": vm.Driver.URL})
	}
	execImpl, pluginClient, e := d.createExecutor(cfg, handle)
	if e != nil {
		return e
	}
	defer func() {
		if err != nil {
			_ = execImpl.Shutdown("", 0)
			if pluginClient != nil {
				pluginClient.Kill()
			}
		}
	}()
	state := driverState{TaskConfig: cfg, StartedAt: h.startedAt, VMName: vm.Name, Build: vm.Build != nil}
	if pluginClient != nil {
		state.ReattachConfig = pstructs.ReattachConfigFromGoPlugin(pluginClient.ReattachConfig())
	}
	if h.build != nil {
		h.stateLock.Lock()
		h.build.Reattach = state.ReattachConfig
		r := *h.build
		h.stateLock.Unlock()
		if e = d.save(r); e != nil {
			return e
		}
	}
	args, e := d.client.BuildStartArgs(vm)
	if e != nil {
		return e
	}
	path, e := d.tartPath()
	if e != nil {
		return e
	}
	ec := &executor.ExecCommand{Cmd: path, Args: args, Env: tartEnvList(cfg), User: cfg.User, TaskDir: cfg.TaskDir().Dir, StdoutPath: cfg.StdoutPath, StderrPath: cfg.StderrPath, NetworkIsolation: cfg.NetworkIsolation}
	if vm.Build != nil {
		ec.Env = vm.Build.hostEnv()
		ec.TaskDir = vm.Build.StateDir
		ec.StdoutPath = os.DevNull
		ec.StderrPath = os.DevNull
		ec.NetworkIsolation = nil
	}
	if e = ctx.Err(); e != nil {
		return e
	}
	ps, e := execImpl.Launch(ec)
	if e != nil {
		return e
	}
	h.stateLock.Lock()
	h.exec = execImpl
	h.pluginClient = pluginClient
	h.pid = ps.Pid
	h.stateLock.Unlock()
	state.Pid = ps.Pid
	// Build handles have already been returned to Nomad. Their executor identity
	// is journaled above; never mutate the returned handle asynchronously.
	if vm.Build == nil {
		handle.State = drivers.TaskStateRunning
		if e = handle.SetDriverState(&state); e != nil {
			return e
		}
	}
	if vm.Build != nil {
		h.startupDone = make(chan struct{})
		go func() {
			defer close(h.startupDone)
			d.executeBuild(ctx, h)
			_ = execImpl.Shutdown("SIGINT", 10*time.Second)
		}()
		return nil
	}
	network, e := d.resolveDriverNetwork(vm)
	if e == nil {
		h.networkOverride = network.Copy()
	}
	if vm.Driver.Command != "" || len(vm.Driver.Args) > 0 {
		startupCtx, cancel := context.WithCancel(ctx)
		h.startupCancel = cancel
		out, e := openTaskLog(cfg.StdoutPath)
		if e != nil {
			return e
		}
		stderr, e := openTaskLog(cfg.StderrPath)
		if e != nil {
			out.Close()
			return e
		}
		go func() {
			defer cancel()
			defer out.Close()
			defer stderr.Close()
			if vm.Driver.GuestAgent {
				if e := d.waitForGuestAgent(startupCtx, vm); e != nil {
					return
				}
			}
			d.executeStartupCommand(startupCtx, cfg.ID, cfg.Name, cfg.AllocID, vm, out, stderr)
		}()
	}
	return nil
}

func (d *Driver) tartPath() (string, error) {
	if d.config.Build != nil {
		return d.config.Build.TartPath, nil
	}
	return exec.LookPath("tart")
}

func (d *Driver) validateTask(cfg *drivers.TaskConfig, tc TaskConfig) error {
	if !d.config.Enabled {
		return fmt.Errorf("driver is disabled")
	}
	if d.config.Build != nil {
		return d.config.Build.validateTask(cfg, tc)
	}
	if tc.Source || tc.Artifacts {
		return fmt.Errorf("source/artifacts require an operator build profile")
	}
	if tc.URL == "" {
		return fmt.Errorf("url is required")
	}
	if !tc.PullOnly && !tc.GuestAgent && (tc.SSHUser == "" || tc.SSHPassword == "") {
		return fmt.Errorf("ssh_user and ssh_password are required for SSH VMs")
	}
	return nil
}

// RecoverTask recreates the in-memory state of a task from a TaskHandle.
func (d *Driver) RecoverTask(h *drivers.TaskHandle) error {
	if h == nil {
		return fmt.Errorf("error: handle cannot be nil")
	}

	if h.Version != taskHandleVersion {
		return fmt.Errorf("error: incompatible handle version of %d", h.Version)
	}

	var taskState driverState
	if err := h.GetDriverState(&taskState); err != nil {
		return fmt.Errorf("failed to decode task state from handle: %w", err)
	}

	if h.Config == nil || taskState.TaskConfig == nil || h.Config.ID != taskState.TaskConfig.ID || h.Config.AllocID != taskState.TaskConfig.AllocID {
		return fmt.Errorf("recovery identity mismatch")
	}
	if _, ok := d.tasks.Get(h.Config.ID); ok {
		return nil
	}
	if d.config.Build != nil || taskState.Build {
		if d.config.Build == nil || !taskState.Build {
			return fmt.Errorf("recovery profile mismatch")
		}
		r, e := readRecord(d.recordPath(h.Config.ID))
		if e != nil {
			return e
		}
		if r.AllocID != h.Config.AllocID || r.CleanupPending {
			return fmt.Errorf("cleanup required or identity mismatch")
		}
		if e := publishBuildResult(h.Config, r); e != nil {
			return fmt.Errorf("publishing recovered build result: %w", e)
		}
		th := &taskHandle{taskConfig: h.Config, build: &r, state: drivers.TaskStateExited, startedAt: r.Started, completedAt: r.Finished, exitResult: buildResult(r), doneCh: make(chan struct{})}
		close(th.doneCh)
		d.tasks.Set(h.Config.ID, th)
		return nil
	}
	if taskState.ReattachConfig == nil {
		return fmt.Errorf("old handle lacks executor identity; refusing to replay task")
	}
	rc, e := pstructs.ReattachConfigToGoPlugin(taskState.ReattachConfig)
	if e != nil {
		return e
	}
	compute := cpustats.Compute{}
	if d.nomadConfig != nil && d.nomadConfig.Topology != nil {
		compute = d.nomadConfig.Topology.Compute()
	}
	ex, pc, e := executor.ReattachToExecutor(rc, d.logger, compute)
	if e != nil {
		return e
	}
	var tc TaskConfig
	if e = h.Config.DecodeDriverConfig(&tc); e != nil {
		pc.Kill()
		return e
	}
	th := &taskHandle{taskConfig: h.Config, vmConfig: VMConfig{Driver: tc, Nomad: h.Config, Name: taskState.VMName}, exec: ex, pluginClient: pc, pid: taskState.Pid, state: drivers.TaskStateRunning, startedAt: taskState.StartedAt, doneCh: make(chan struct{}), logger: d.logger, pullOnly: taskState.PullOnly}
	d.tasks.Set(h.Config.ID, th)
	go th.run()
	return nil
}

// WaitTask returns a channel used to notify Nomad when a task exits.
func (d *Driver) WaitTask(ctx context.Context, taskID string) (<-chan *drivers.ExitResult, error) {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	ch := make(chan *drivers.ExitResult)
	go d.handleWait(ctx, handle, ch)
	return ch, nil
}

// StopTask stops a running task with the given signal and within the timeout window.
func (d *Driver) StopTask(taskID string, timeout time.Duration, signal string) error {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}

	if handle.build != nil {
		if handle.shutdown != nil {
			handle.shutdown()
		}
		select {
		case <-handle.doneCh:
		case <-time.After(90 * time.Second):
			return fmt.Errorf("task shutdown timed out")
		}
		handle.stateLock.RLock()
		pending := handle.build != nil && handle.build.CleanupPending
		handle.stateLock.RUnlock()
		if pending {
			return fmt.Errorf("VM cleanup pending")
		}
		return nil
	}
	var allocVMName string
	if handle.taskConfig != nil && !handle.pullOnly {
		allocVMName = handle.vmName()
		if err := d.client.Stop(d.ctx, allocVMName, timeout); err != nil {
			return fmt.Errorf("VM stop failed: %w", err)
		}
	} else if handle.taskConfig == nil {
		d.logger.Warn("task config missing while stopping task", "task_id", taskID)
	}

	if err := handle.exec.Shutdown(signal, timeout); err != nil {
		if handle.pluginClient != nil && handle.pluginClient.Exited() {
			return nil
		}
		return fmt.Errorf("executor Shutdown failed: %w", err)
	}

	<-handle.doneCh

	if handle.pluginClient != nil {
		handle.pluginClient.Kill()
	} else {
		handle.logger.Warn("plugin client missing while stopping task")
	}

	if allocVMName != "" {
		if err := d.client.Delete(d.ctx, allocVMName); err != nil {
			return fmt.Errorf("VM deletion failed: %w", err)
		}
	}

	d.logger.Info("stopped tart task", "task_id", taskID)
	return nil
}

// DestroyTask cleans up and removes a task that has terminated.
func (d *Driver) DestroyTask(taskID string, force bool) error {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}

	if handle.shutdown != nil && handle.IsRunning() && force {
		if e := d.StopTask(taskID, 10*time.Second, ""); e != nil {
			return e
		}
	}
	if handle.IsRunning() && !force {
		return fmt.Errorf("cannot destroy running task")
	}

	if handle.build != nil {
		handle.stateLock.RLock()
		r := *handle.build
		handle.stateLock.RUnlock()
		if r.CleanupPending {
			if e := d.cleanupBuild(&r); e != nil {
				return e
			}
			if e := d.save(r); e != nil {
				return e
			}
			d.admission.Lock()
			d.build.busy = false
			d.admission.Unlock()
		}
		if e := os.Remove(d.recordPath(taskID)); e != nil && !errors.Is(e, os.ErrNotExist) {
			return e
		}
		d.tasks.Delete(taskID)
		return nil
	}
	if handle.pluginClient != nil && !handle.pluginClient.Exited() {
		if handle.exec != nil {
			if err := handle.exec.Shutdown("", 0); err != nil {
				handle.logger.Error("destroying executor failed", "error", err)
			}
		} else {
			handle.logger.Warn("executor missing while destroying task")
		}
		handle.pluginClient.Kill()
	}

	if handle.taskConfig != nil && !handle.pullOnly {
		allocVMName := handle.vmName()
		if err := d.client.Delete(d.ctx, allocVMName); err != nil {
			return fmt.Errorf("VM deletion failed: %w", err)
		}
	} else if handle.taskConfig == nil {
		d.logger.Warn("task config missing while destroying task", "task_id", taskID)
	}

	d.tasks.Delete(taskID)
	d.logger.Info("destroyed tart task", "task_id", taskID)
	return nil
}

// InspectTask returns detailed status information for the referenced taskID.
func (d *Driver) InspectTask(taskID string) (*drivers.TaskStatus, error) {
	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	return handle.TaskStatus(), nil
}

// TaskStats returns a channel which the driver should send stats to at the given interval.
func (d *Driver) TaskStats(ctx context.Context, taskID string, interval time.Duration) (<-chan *drivers.TaskResourceUsage, error) {
	h, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}
	if h.build != nil {
		return d.buildStats(ctx, h, interval)
	}
	return h.exec.Stats(ctx, interval)
}

// TaskEvents returns a channel that the plugin can use to emit task related events.
func (d *Driver) TaskEvents(ctx context.Context) (<-chan *drivers.TaskEvent, error) {
	return d.eventer.TaskEvents(ctx)
}

// SignalTask forwards a signal to a task.
func (d *Driver) SignalTask(taskID string, signal string) error {
	if d.config.Build != nil {
		return fmt.Errorf("signals disabled in build profile")
	}
	_, ok := d.tasks.Get(taskID)
	if !ok {
		return drivers.ErrTaskNotFound
	}

	// TODO: Implement actual VM signaling logic
	d.logger.Info("signaling tart task", "task_id", taskID, "signal", signal)
	return nil
}

// ExecTask returns the result of executing the given command inside a task.
func (d *Driver) ExecTask(taskID string, cmd []string, timeout time.Duration) (*drivers.ExecTaskResult, error) {
	_, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	// Exec is not supported
	return nil, fmt.Errorf("exec is not supported by the tart driver")
}

// ExecTaskStreaming executes a command inside the VM backing the allocation and
// streams the input and output over the provided ExecOptions. The VM is
// contacted over SSH and the session will remain active for the lifetime of the
// context.
func (d *Driver) ExecTaskStreaming(ctx context.Context, taskID string, opts *drivers.ExecOptions) (*drivers.ExitResult, error) {
	defer opts.Stdout.Close()
	defer opts.Stderr.Close()
	defer opts.Stdin.Close()

	handle, ok := d.tasks.Get(taskID)
	if !ok {
		return nil, drivers.ErrTaskNotFound
	}

	if d.config.Build != nil {
		return nil, fmt.Errorf("interactive exec disabled in build profile")
	}
	var taskCfg TaskConfig
	if err := handle.taskConfig.DecodeDriverConfig(&taskCfg); err != nil {
		return nil, fmt.Errorf("failed to decode driver config: %w", err)
	}

	execOptions := ExecOptions{
		Command:  opts.Command,
		Tty:      opts.Tty,
		Stdin:    opts.Stdin,
		Stdout:   opts.Stdout,
		Stderr:   opts.Stderr,
		ResizeCh: opts.ResizeCh,
	}

	vmConfig := VMConfig{
		Driver: taskCfg,
		Nomad:  handle.taskConfig,
		Name:   handle.vmName(),
	}

	exitCode, err := d.client.Exec(ctx, vmConfig, execOptions)
	if err != nil {
		return nil, fmt.Errorf("failed to exec command: %w", err)
	}

	return &drivers.ExitResult{ExitCode: exitCode}, nil
}

func (h *taskHandle) vmName() string {
	if h.vmConfig.Name != "" {
		return h.vmConfig.Name
	}
	return vmName(h.taskConfig.AllocID)
}
