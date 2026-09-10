package driver

import (
	"context"
	"encoding/json"
	"github.com/hashicorp/nomad/plugins/drivers"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRecoveryRemovesInterruptedExport(t *testing.T) {
	for _, suffix := range []string{".tar.partial", ".tar"} {
		t.Run(suffix, func(t *testing.T) {
			d, cfg := buildFixture(t)
			r := newRecord(cfg.ID, cfg.AllocID, d.config.Build.Image)
			path := filepath.Join(cfg.TaskDir().LocalDir, r.VM+suffix)
			foreign := filepath.Join(cfg.TaskDir().LocalDir, "unrelated.tar.partial")
			for _, p := range []string{path, foreign} {
				if e := os.WriteFile(p, []byte("interrupted export"), 0600); e != nil {
					t.Fatal(e)
				}
			}
			if e := d.save(r); e != nil {
				t.Fatal(e)
			}
			if e := d.reconcileBuilds(); e != nil {
				t.Fatal(e)
			}
			h := drivers.NewTaskHandle(taskHandleVersion)
			h.Config = cfg
			if e := h.SetDriverState(&driverState{TaskConfig: cfg, Build: true}); e != nil {
				t.Fatal(e)
			}
			if e := d.RecoverTask(h); e != nil {
				t.Fatal(e)
			}
			if _, e := os.Lstat(path); !os.IsNotExist(e) {
				t.Fatalf("recovered task retained incomplete artifact: %v", e)
			}
			if b, e := os.ReadFile(foreign); e != nil || string(b) != "interrupted export" {
				t.Fatal("unrelated artifact changed", e)
			}
			if result := waitBuildTest(t, d, cfg.ID); result.ExitCode != -1 || result.Err == nil {
				t.Fatal("interruption lost", result)
			}
		})
	}
}

func TestJournaledExportRecoveryAndRetry(t *testing.T) {
	for _, blocker := range []string{"none", "symlink", "directory", "hardlink"} {
		t.Run(blocker, func(t *testing.T) {
			d, cfg := buildFixture(t)
			r := newRecord(cfg.ID, cfg.AllocID, d.config.Build.Image)
			r.ArtifactDir = cfg.TaskDir().LocalDir
			path := filepath.Join(r.ArtifactDir, r.VM+".tar.partial")
			foreign := filepath.Join(d.config.Build.StateDir, "preserved")
			if e := os.WriteFile(foreign, []byte("preserved"), 0600); e != nil {
				t.Fatal(e)
			}
			var e error
			switch blocker {
			case "none":
				e = os.WriteFile(path, []byte("unfinished"), 0600)
			case "symlink":
				e = os.Symlink(foreign, path)
			case "directory":
				e = os.Mkdir(path, 0700)
			case "hardlink":
				e = os.Link(foreign, path)
			}
			if e != nil {
				t.Fatal(e)
			}
			if e = d.save(r); e != nil {
				t.Fatal(e)
			}
			e = d.reconcileBuilds()
			if blocker != "none" {
				if e == nil {
					t.Fatal("unexpected artifact object accepted")
				}
				saved, err := readRecord(d.recordPath(cfg.ID))
				if err != nil || !saved.CleanupPending || saved.ArtifactDir == "" {
					t.Fatal("cleanup ownership forgotten", saved, err)
				}
				if b, err := os.ReadFile(foreign); err != nil || string(b) != "preserved" {
					t.Fatal("foreign object changed", err)
				}
				if e = os.Remove(path); e != nil {
					t.Fatal(e)
				}
				if e = os.WriteFile(path, []byte("unfinished"), 0600); e != nil {
					t.Fatal(e)
				}
				e = d.reconcileBuilds()
			}
			if e != nil {
				t.Fatal(e)
			}
			if _, e = os.Lstat(path); !os.IsNotExist(e) {
				t.Fatal("startup reconciliation retained partial", e)
			}
			saved, e := readRecord(d.recordPath(cfg.ID))
			if e != nil || saved.CleanupPending || saved.ArtifactDir != "" {
				t.Fatal("incomplete cleanup", saved, e)
			}
			raw, e := os.ReadFile(d.recordPath(cfg.ID))
			if e != nil || strings.Contains(string(raw), "ArtifactDir") {
				t.Fatal("completed journal breaks older-reader compatibility", e)
			}
		})
	}
}

func TestLegacyArtifactCleanupRetryRestoresAdmission(t *testing.T) {
	for _, otherPending := range []bool{false, true} {
		t.Run(map[bool]string{false: "idle", true: "other-pending"}[otherPending], func(t *testing.T) {
			d, cfg := buildFixture(t)
			r := newRecord(cfg.ID, cfg.AllocID, d.config.Build.Image)
			r.CleanupPending = false
			r.Phase = "complete"
			r.Failure = "build interrupted by driver/agent restart"
			if e := d.save(r); e != nil {
				t.Fatal(e)
			}
			partial := filepath.Join(cfg.TaskDir().LocalDir, r.VM+".tar.partial")
			foreign := filepath.Join(d.config.Build.StateDir, "preserved")
			if e := os.WriteFile(foreign, []byte("preserved"), 0600); e != nil {
				t.Fatal(e)
			}
			if e := os.Symlink(foreign, partial); e != nil {
				t.Fatal(e)
			}
			h := drivers.NewTaskHandle(taskHandleVersion)
			h.Config = cfg
			if e := h.SetDriverState(&driverState{TaskConfig: cfg, Build: true}); e != nil {
				t.Fatal(e)
			}
			if e := d.RecoverTask(h); e == nil || !d.build.busy {
				t.Fatal("cleanup failure did not block admission", e)
			}
			if e := os.Remove(partial); e != nil {
				t.Fatal(e)
			}
			if otherPending {
				if e := d.save(newRecord("other", "alloc-other", d.config.Build.Image)); e != nil {
					t.Fatal(e)
				}
			}
			if e := d.RecoverTask(h); e != nil {
				t.Fatal(e)
			}
			if d.build.busy != otherPending {
				t.Fatal("incorrect recovered admission", d.build.busy)
			}
			raw, e := os.ReadFile(filepath.Join(cfg.TaskDir().LocalDir, "build-result.json"))
			if e != nil {
				t.Fatal(e)
			}
			var saved buildRecord
			if e = json.Unmarshal(raw, &saved); e != nil {
				t.Fatal(e)
			}
			if saved.CleanupPending || saved.Phase != "complete" || saved.Finished.IsZero() {
				t.Fatal("incoherent recovered result", saved)
			}
		})
	}
}

func TestExportJournalsDirectoryBeforeGuestTransfer(t *testing.T) {
	d, cfg := buildFixture(t)
	script := strings.Replace(fakeTart, "/usr/bin/tar) printf 'artifact bytes';;", "/usr/bin/tar) touch \"$HOME/export-started\"; sleep 120;;", 1)
	if e := os.WriteFile(d.config.Build.TartPath, []byte(script), 0700); e != nil {
		t.Fatal(e)
	}
	startBuildTest(t, d, cfg, "/guest/ok")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	for {
		if _, e := os.Stat(filepath.Join(d.config.Build.StateDir, "export-started")); e == nil {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("export did not start")
		case <-time.After(20 * time.Millisecond):
		}
	}
	r, e := readRecord(d.recordPath(cfg.ID))
	if e != nil || r.ArtifactDir != cfg.TaskDir().LocalDir || !r.CleanupPending {
		t.Fatal("export began without durable cleanup directory", r, e)
	}
	if e = d.StopTask(cfg.ID, time.Second, ""); e != nil {
		t.Fatal(e)
	}
	if result := waitBuildTest(t, d, cfg.ID); result.Err == nil {
		t.Fatal("canceled export succeeded")
	}
	r, e = readRecord(d.recordPath(cfg.ID))
	if e != nil || r.ArtifactDir != "" || r.CleanupPending {
		t.Fatal("export cleanup incomplete", r, e)
	}
}

func TestRecoveryPreservesCommittedArtifact(t *testing.T) {
	d, cfg := buildFixture(t)
	r := newRecord(cfg.ID, cfg.AllocID, d.config.Build.Image)
	r.ArtifactDir = cfg.TaskDir().LocalDir
	r.Artifact = r.VM + ".tar"
	r.ArtifactSHA256 = strings.Repeat("a", 64)
	archive := filepath.Join(r.ArtifactDir, r.Artifact)
	partial := archive + ".partial"
	for _, p := range []string{archive, partial} {
		if e := os.WriteFile(p, []byte("committed"), 0600); e != nil {
			t.Fatal(e)
		}
	}
	if e := d.save(r); e != nil {
		t.Fatal(e)
	}
	if e := d.reconcileBuilds(); e != nil {
		t.Fatal(e)
	}
	if b, e := os.ReadFile(archive); e != nil || string(b) != "committed" {
		t.Fatal("committed archive changed", e)
	}
	if _, e := os.Lstat(partial); !os.IsNotExist(e) {
		t.Fatal("partial retained", e)
	}
}

func TestExportJournalFailureCreatesNoArtifact(t *testing.T) {
	d, cfg := buildFixture(t)
	r := newRecord(cfg.ID, cfg.AllocID, d.config.Build.Image)
	records := filepath.Join(d.config.Build.StateDir, "records")
	if e := os.Rename(records, records+".saved"); e != nil {
		t.Fatal(e)
	}
	if e := os.WriteFile(records, nil, 0600); e != nil {
		t.Fatal(e)
	}
	h := &taskHandle{taskConfig: cfg, build: &r, vmConfig: VMConfig{Name: r.VM, Build: d.config.Build}, shutdown: func() {}}
	if e := d.collectBuild(context.Background(), h); e == nil {
		t.Fatal("export continued without journal")
	}
	entries, e := os.ReadDir(cfg.TaskDir().LocalDir)
	if e != nil || len(entries) != 0 {
		t.Fatal("artifact created before journal", entries, e)
	}
	if _, e := os.Stat(filepath.Join(d.config.Build.StateDir, "calls")); !os.IsNotExist(e) {
		t.Fatal("guest export invoked before journal", e)
	}
}

func TestRecoveryRejectsArtifactDirectoryMismatch(t *testing.T) {
	d, cfg := buildFixture(t)
	r := newRecord(cfg.ID, cfg.AllocID, d.config.Build.Image)
	if e := d.save(r); e != nil {
		t.Fatal(e)
	}
	h := drivers.NewTaskHandle(taskHandleVersion)
	h.Config = cfg.Copy()
	h.Config.AllocDir = filepath.Join(d.config.Build.StateDir, "different")
	if e := h.SetDriverState(&driverState{TaskConfig: cfg, Build: true}); e != nil {
		t.Fatal(e)
	}
	if e := d.RecoverTask(h); e == nil || !strings.Contains(e.Error(), "identity mismatch") {
		t.Fatal("changed task directory accepted", e)
	}
}

func TestCompletingBuildCannotClearOlderRecoveryBlock(t *testing.T) {
	d, cfg := buildFixture(t)
	old := cfg.Copy()
	old.ID = "older-task"
	old.AllocID = "older-alloc"
	r := newRecord(old.ID, old.AllocID, d.config.Build.Image)
	r.CleanupPending = false
	r.Phase = "complete"
	r.Failure = "interrupted"
	if e := d.save(r); e != nil {
		t.Fatal(e)
	}
	partial := filepath.Join(old.TaskDir().LocalDir, r.VM+".tar.partial")
	if e := os.Mkdir(partial, 0700); e != nil {
		t.Fatal(e)
	}
	startBuildTest(t, d, cfg, "/guest/hang")
	deadline := time.Now().Add(5 * time.Second)
	for {
		if _, e := os.Stat(filepath.Join(d.config.Build.StateDir, "guest-started")); e == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("guest did not start")
		}
		time.Sleep(20 * time.Millisecond)
	}
	h := drivers.NewTaskHandle(taskHandleVersion)
	h.Config = old
	if e := h.SetDriverState(&driverState{TaskConfig: old, Build: true}); e != nil {
		t.Fatal(e)
	}
	if e := d.RecoverTask(h); e == nil {
		t.Fatal("unsafe older export accepted")
	}
	if e := d.StopTask(cfg.ID, time.Second, ""); e != nil {
		t.Fatal(e)
	}
	waitBuildTest(t, d, cfg.ID)
	if !d.build.busy {
		t.Fatal("completion cleared an older cleanup block")
	}
	if e := d.DestroyTask(cfg.ID, false); e != nil {
		t.Fatal(e)
	}
	if !d.build.busy {
		t.Fatal("destruction cleared an older cleanup block")
	}
	if e := os.Remove(partial); e != nil {
		t.Fatal(e)
	}
	if e := d.RecoverTask(h); e != nil {
		t.Fatal(e)
	}
	if d.build.busy {
		t.Fatal("successful final recovery did not release admission")
	}
}
