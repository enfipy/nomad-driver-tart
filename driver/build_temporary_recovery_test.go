package driver

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func TestTemporaryStagingBlocksRecoveryUntilActuallyGone(t *testing.T) {
	d, cfg := buildFixture(t)
	r := newRecord(cfg.ID, cfg.AllocID, d.config.Build.Image)
	if e := d.save(r); e != nil {
		t.Fatal(e)
	}
	tmp := filepath.Join(d.config.Build.StateDir, "vms", "tmp")
	partial := filepath.Join(tmp, "interrupted-clone")
	if e := os.MkdirAll(partial, 0700); e != nil {
		t.Fatal(e)
	}
	sentinel := filepath.Join(d.config.Build.StateDir, "preserved")
	if e := os.WriteFile(sentinel, []byte("keep"), 0600); e != nil {
		t.Fatal(e)
	}
	// Model Tart list succeeding after its best-effort GC fails or skips a lock.
	if e := d.reconcileBuilds(); e == nil {
		t.Fatal("reported cleanup complete with temporary staging still present")
	}
	saved, e := readRecord(d.recordPath(cfg.ID))
	if e != nil || !saved.CleanupPending {
		t.Fatalf("lost pending ownership: %+v %v", saved, e)
	}
	if _, e = os.Stat(partial); e != nil {
		t.Fatal("driver performed an unowned staging sweep", e)
	}
	if e = os.Remove(partial); e != nil {
		t.Fatal(e)
	}
	if e = d.reconcileBuilds(); e != nil {
		t.Fatal(e)
	}
	saved, e = readRecord(d.recordPath(cfg.ID))
	if e != nil || saved.CleanupPending || saved.Phase != "complete" {
		t.Fatalf("retry did not complete: %+v %v", saved, e)
	}
	if contents, e := os.ReadFile(sentinel); e != nil || string(contents) != "keep" {
		t.Fatal("unrelated file changed", e)
	}
}

func TestTemporaryStagingRejectsSymlinkWithoutTouchingTarget(t *testing.T) {
	d, _ := buildFixture(t)
	target := filepath.Join(d.config.Build.StateDir, "preserved")
	if e := os.Mkdir(target, 0700); e != nil {
		t.Fatal(e)
	}
	leaf := filepath.Join(target, "keep")
	if e := os.WriteFile(leaf, []byte("keep"), 0600); e != nil {
		t.Fatal(e)
	}
	if e := os.Symlink(target, filepath.Join(d.config.Build.StateDir, "vms", "tmp")); e != nil {
		t.Fatal(e)
	}
	marker := filepath.Join(d.config.Build.StateDir, "tart-was-invoked")
	if e := os.WriteFile(d.config.Build.TartPath, []byte(fmt.Sprintf("#!/bin/sh\ntouch '%s'\nexit 0\n", marker)), 0700); e != nil {
		t.Fatal(e)
	}
	if e := d.cleanupVM("cloud-test"); e == nil {
		t.Fatal("accepted symlink staging")
	}
	if _, e := os.Stat(marker); !os.IsNotExist(e) {
		t.Fatal("Tart GC was invoked before staging validation")
	}
	if b, e := os.ReadFile(leaf); e != nil || string(b) != "keep" {
		t.Fatal("changed target", e)
	}
}
