package driver

import (
	"context"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func TestKernelProcessPathDoesNotTrustArgvZero(t *testing.T) {
	cmd := exec.Command("/bin/sleep", "30")
	cmd.Args[0] = vmHelperPath
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	path, err := kernelProcessPath(int32(cmd.Process.Pid))
	want, _ := filepath.EvalSymlinks("/bin/sleep")
	if err != nil || path != want {
		t.Fatalf("kernel executable path=%q, err=%v; want %q", path, err, want)
	}
}

// Opt-in, read-only qualification under the same dedicated UID as a live Tart
// build. The operator supplies the verified Tart run PID and immutable binary.
func TestLiveBuildStats(t *testing.T) {
	text := os.Getenv("TART_STATS_PID")
	if text == "" {
		t.Skip("no live build selected")
	}
	if os.Geteuid() == 0 {
		t.Fatal("qualify accounting as the unprivileged build UID")
	}
	pid, err := strconv.ParseInt(text, 10, 32)
	if err != nil || pid <= 0 {
		t.Fatal("positive Tart PID required")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var counter buildUsageCounter
	for i := 0; i < 2; i++ {
		ps, err := readBuildProcesses(ctx, int32(pid), os.Getenv("TART_STATS_PATH"), time.Unix(0, 0))
		if err != nil {
			t.Fatal(err)
		}
		sample, err := counter.sample(ps, time.Now())
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("processes=%d tart_pid=%d helper_pid=%d RSS_bytes=%d CPU_percent=%.2f CPU_measured=%v", len(ps), ps[0].ID.PID, ps[1].ID.PID, sample.ResourceUsage.MemoryStats.RSS, sample.ResourceUsage.CpuStats.Percent, sample.ResourceUsage.CpuStats.Measured)
		if i == 0 {
			time.Sleep(time.Second)
		}
	}
}
