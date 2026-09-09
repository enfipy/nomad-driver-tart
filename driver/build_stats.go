package driver

import (
	"fmt"
	"time"

	"github.com/hashicorp/nomad/plugins/drivers"
)

const vmHelperPath = "/System/Library/Frameworks/Virtualization.framework/Versions/A/XPCServices/com.apple.Virtualization.VirtualMachine.xpc/Contents/MacOS/com.apple.Virtualization.VirtualMachine"

type processIdentity struct {
	PID       int32
	StartedNS int64
}

type buildProcess struct {
	ID         processIdentity
	Parent     int32
	UID        uint32
	Path       string
	CPUSeconds float64
	RSS        uint64
}

// A build profile admits one VM at a time under a dedicated UID. Apple's VM
// helper is parented by launchd, not Tart. Require a single, newly-created helper
// and the exact live Tart instance; never aggregate processes by name alone.
func selectBuildProcesses(pid int32, tartPath string, started time.Time, all []buildProcess) ([]buildProcess, error) {
	var root *buildProcess
	for i := range all {
		if all[i].ID.PID == pid {
			root = &all[i]
			break
		}
	}
	if root == nil || root.Path != tartPath || root.ID.StartedNS < started.UnixNano() {
		return nil, fmt.Errorf("Tart process identity unavailable")
	}
	var helper *buildProcess
	for i := range all {
		p := &all[i]
		if p.UID != root.UID || p.Path != vmHelperPath {
			continue
		}
		if helper != nil || p.ID.StartedNS < root.ID.StartedNS {
			return nil, fmt.Errorf("VM helper attribution is ambiguous")
		}
		helper = p
	}
	if helper == nil {
		return nil, fmt.Errorf("VM helper unavailable")
	}
	selected := []buildProcess{*root, *helper}
	parents := map[int32]bool{pid: true}
	// Include Tart's descendants (not the Nomad client, plugin or other VMs).
	for changed := true; changed; {
		changed = false
		for _, p := range all {
			if p.UID == root.UID && !parents[p.ID.PID] && p.ID != helper.ID && parents[p.Parent] && p.ID.StartedNS >= root.ID.StartedNS {
				parents[p.ID.PID] = true
				selected = append(selected, p)
				changed = true
			}
		}
	}
	return selected, nil
}

func soleRunningBuildVM(vms []VMInfo, name string) bool {
	found := false
	for _, vm := range vms {
		if vm.Source == "oci" || vm.Status == VMStateStopped {
			continue
		}
		if vm.Name != name || vm.Status != VMStateRunning || found {
			return false
		}
		found = true
	}
	return found
}

type buildUsageCounter struct {
	previous     map[processIdentity]float64
	at           time.Time
	root, helper processIdentity
}

func (c *buildUsageCounter) sample(ps []buildProcess, now time.Time) (*drivers.TaskResourceUsage, error) {
	if len(ps) < 2 {
		return nil, fmt.Errorf("incomplete VM sample")
	}
	if c.previous != nil && (ps[0].ID != c.root || ps[1].ID != c.helper) {
		return nil, fmt.Errorf("VM process instance changed")
	}
	c.root, c.helper = ps[0].ID, ps[1].ID
	cpu := &drivers.CpuStats{}
	mem := &drivers.MemoryStats{Measured: []string{"RSS"}}
	next := make(map[processIdentity]float64, len(ps))
	elapsed := now.Sub(c.at).Seconds()
	validCPU := c.previous != nil && elapsed > 0
	for _, p := range ps {
		mem.RSS += p.RSS
		next[p.ID] = p.CPUSeconds
		old, exists := c.previous[p.ID]
		if !exists || p.CPUSeconds < old {
			validCPU = false
			continue
		}
		cpu.Percent += 100 * (p.CPUSeconds - old) / elapsed
	}
	if validCPU {
		cpu.Measured = []string{"Percent"}
	} else {
		cpu.Percent = 0
	}
	c.previous, c.at = next, now
	return &drivers.TaskResourceUsage{Timestamp: now.UnixNano(), ResourceUsage: &drivers.ResourceUsage{CpuStats: cpu, MemoryStats: mem}}, nil
}
