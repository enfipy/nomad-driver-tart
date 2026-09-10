package driver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestDiskAdmissionAccountsForCacheAndReserve(t *testing.T) {
	c := BuildConfig{ImageDiskMB: 133515, MinFreeDiskMB: 32768}
	for _, tc := range []struct {
		free         uint64
		download, ok bool
	}{
		{170 * 1024 * 1024 * 1024, true, true},
		{160 * 1024 * 1024 * 1024, true, false},
		{40 * 1024 * 1024 * 1024, false, true},
		{31 * 1024 * 1024 * 1024, false, false},
	} {
		if e := c.checkDisk(tc.free, tc.download); (e == nil) != tc.ok {
			t.Fatalf("%+v: %v", tc, e)
		}
	}
}

func TestDiskMonitorStopsAtReserveAndFailsClosed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	calls := 0
	e := monitorDisk(ctx, time.Millisecond, 32, func() (uint64, error) {
		calls++
		if calls == 1 {
			return 64, nil
		}
		return 31, nil
	})
	if e == nil || !strings.Contains(e.Error(), "reserve") || calls != 2 {
		t.Fatalf("calls=%d error=%v", calls, e)
	}
	e = monitorDisk(ctx, time.Millisecond, 32, func() (uint64, error) { return 0, errors.New("volume unavailable") })
	if e == nil || !strings.Contains(e.Error(), "volume unavailable") {
		t.Fatal(e)
	}
	cancel()
	if e = monitorDisk(ctx, time.Hour, 32, func() (uint64, error) { t.Fatal("read after cancellation"); return 0, nil }); e != nil {
		t.Fatal(e)
	}
}

func TestDiskAdmissionFailureDoesNotClone(t *testing.T) {
	d, cfg := buildFixture(t)
	d.config.Build.ImageDiskMB = 16 * 1024 * 1024
	startBuildTest(t, d, cfg, "/usr/bin/true")
	result := waitBuildTest(t, d, cfg.ID)
	if result.Err == nil || !strings.Contains(result.Err.Error(), "insufficient build storage") {
		t.Fatalf("%+v", result)
	}
	calls, _ := os.ReadFile(filepath.Join(d.config.Build.StateDir, "calls"))
	if strings.Contains(string(calls), "clone") {
		t.Fatal("cloned despite insufficient capacity")
	}
}

func TestDiskCancellationPreservesCauseAndCleansRunningGuest(t *testing.T) {
	d, cfg := buildFixture(t)
	startBuildTest(t, d, cfg, "/guest/hang")
	deadline := time.Now().Add(4 * time.Second)
	for {
		if _, e := os.Stat(filepath.Join(d.config.Build.StateDir, "guest-started")); e == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("guest did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
	h, ok := d.tasks.Get(cfg.ID)
	if !ok {
		t.Fatal("missing handle")
	}
	cause := monitorDisk(context.Background(), time.Millisecond, 32, func() (uint64, error) { return 31, nil })
	h.failBuild(cause)
	h.shutdown()
	r := waitBuildTest(t, d, cfg.ID)
	if r.Err == nil || !strings.Contains(r.Err.Error(), cause.Error()) {
		t.Fatalf("lost initiating failure: %+v", r)
	}
	if _, e := os.Stat(filepath.Join(d.config.Build.StateDir, "vm-name")); !os.IsNotExist(e) {
		t.Fatal("VM survived disk cancellation")
	}
}
