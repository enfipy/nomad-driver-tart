package driver

import (
	"testing"
	"time"

	"github.com/hashicorp/nomad/client/lib/cpustats"
)

func statsFixture() []buildProcess {
	return []buildProcess{
		{ID: processIdentity{10, 100}, UID: 450, Path: "/tart", CPUSeconds: 1, RSS: 3},
		{ID: processIdentity{11, 101}, Parent: 1, UID: 450, Path: vmHelperPath, CPUSeconds: 10, RSS: 200},
		{ID: processIdentity{12, 102}, Parent: 10, UID: 450, Path: "/softnet", CPUSeconds: 1, RSS: 4},
		{ID: processIdentity{13, 101}, Parent: 1, UID: 501, Path: vmHelperPath, CPUSeconds: 999, RSS: 9999},
		{ID: processIdentity{14, 1}, Parent: 1, UID: 450, Path: "/nomad", CPUSeconds: 999, RSS: 9999},
	}
}

func TestBuildUsageIncludesVMExcludesOtherWorkloads(t *testing.T) {
	ps, err := selectBuildProcesses(10, "/tart", time.Unix(0, 90), statsFixture())
	if err != nil || len(ps) != 3 {
		t.Fatalf("selection: %v %v", ps, err)
	}
	counter := buildUsageCounter{compute: cpustats.Compute{TotalCompute: 20000, NumCores: 10}}
	first, err := counter.sample(ps, time.Unix(10, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(first.ResourceUsage.CpuStats.Measured) != 0 {
		t.Fatal("first CPU sample is not a measured zero")
	}
	ps[1].CPUSeconds += 3 // VM consumes three CPU seconds in one wall second.
	second, err := counter.sample(ps, time.Unix(11, 0))
	if err != nil {
		t.Fatal(err)
	}
	if second.ResourceUsage.CpuStats.Percent != 300 || second.ResourceUsage.CpuStats.TotalTicks != 6000 || second.ResourceUsage.MemoryStats.RSS != 207 {
		t.Fatalf("VM usage omitted or unrelated work counted: %+v", second.ResourceUsage)
	}
	if m := second.ResourceUsage.MemoryStats.Measured; len(m) != 1 || m[0] != "RSS" {
		t.Fatal("page-ins must not be reported as swap bytes")
	}
	ps[1].ID.StartedNS++ // Reused PID is a different VM, even with the same name.
	if _, err := counter.sample(ps, time.Unix(12, 0)); err == nil {
		t.Fatal("accepted reused helper PID")
	}
}

func TestBuildUsageRejectsAmbiguousAndStaleHelpers(t *testing.T) {
	for _, scenario := range []string{"missing", "old", "second", "wrong-root", "old-root"} {
		t.Run(scenario, func(t *testing.T) {
			ps := statsFixture()
			switch scenario {
			case "missing":
				ps[1].Path = "/not-the-helper"
			case "old":
				ps[1].ID.StartedNS = 99
			case "second":
				other := ps[1]
				other.ID.PID = 20
				ps = append(ps, other)
			case "wrong-root":
				ps[0].Path = "/other/tart"
			case "old-root":
				ps[0].ID.StartedNS = 89
			}
			if _, err := selectBuildProcesses(10, "/tart", time.Unix(0, 90), ps); err == nil {
				t.Fatal("ambiguous usage must be unavailable")
			}
		})
	}
	if soleRunningBuildVM([]VMInfo{{Name: "owned", Status: VMStateRunning}, {Name: "foreign", Status: VMStatePaused}}, "owned") {
		t.Fatal("accepted another active VM in the store")
	}
	if soleRunningBuildVM(nil, "owned") {
		t.Fatal("missing VM is not running")
	}
	if !soleRunningBuildVM([]VMInfo{{Name: "owned", Status: VMStateRunning}, {Name: "cached", Source: "oci", Status: VMStateStopped}}, "owned") {
		t.Fatal("cached image is not another workload")
	}
}

func TestBuildCPUCounterResetIsUnavailable(t *testing.T) {
	ps := statsFixture()[:2]
	var c buildUsageCounter
	_, _ = c.sample(ps, time.Unix(10, 0))
	ps[1].CPUSeconds = 0
	sample, err := c.sample(ps, time.Unix(11, 0))
	if err != nil || len(sample.ResourceUsage.CpuStats.Measured) != 0 {
		t.Fatal("counter reset reported as measured CPU")
	}
}
