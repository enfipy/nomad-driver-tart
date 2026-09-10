package driver

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"
)

// Opt-in: uses a fresh private Tart home and a 1 GiB sparse Linux disk, without
// booting a VM, downloading an image, or accessing the installed image cache.
func TestRealTartInterruptedStaging(t *testing.T) {
	binary := os.Getenv("TART_QUALIFICATION_BINARY")
	if binary == "" {
		t.Skip("set TART_QUALIFICATION_BINARY to the pinned Tart executable")
	}
	for _, operation := range []string{"clone", "pull"} {
		for _, fault := range []string{"cancel", "parent-death"} {
			t.Run(operation+"/"+fault, func(t *testing.T) {
				d, cfg := buildFixture(t)
				d.config.Build.TartPath = binary
				runner := buildRunner{d.config.Build, d.build}
				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()
				if out, e := runner.Run(ctx, "tart", "create", "--linux", "--disk-size", "1", "source").CombinedOutput(); e != nil {
					t.Fatalf("create: %v %s", e, out)
				}
				source := filepath.Join(d.config.Build.StateDir, "vms", "vms", "source", "config.json")
				before, e := os.ReadFile(source)
				if e != nil {
					t.Fatal(e)
				}
				home, e := os.Open(filepath.Join(d.config.Build.StateDir, "vms"))
				if e != nil {
					t.Fatal(e)
				}
				defer home.Close()
				if e = syscall.Flock(int(home.Fd()), syscall.LOCK_EX); e != nil {
					t.Fatal(e)
				}
				defer syscall.Flock(int(home.Fd()), syscall.LOCK_UN)
				r := newRecord(cfg.ID, cfg.AllocID, d.config.Build.Image)
				if e = d.save(r); e != nil {
					t.Fatal(e)
				}
				args := []string{"clone", "source", r.VM}
				blobRequested := make(chan struct{})
				var blobOnce sync.Once
				if operation == "pull" {
					server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
						if strings.Contains(req.URL.Path, "/manifests/") {
							w.Header().Set("Content-Type", "application/vnd.oci.image.manifest.v1+json")
							fmt.Fprintf(w, `{"schemaVersion":2,"mediaType":"application/vnd.oci.image.manifest.v1+json","config":{"mediaType":"application/vnd.oci.image.config.v1+json","size":2,"digest":"sha256:%s"},"layers":[{"mediaType":"application/vnd.cirruslabs.tart.config.v1","size":4096,"digest":"sha256:%s"}]}`, strings.Repeat("a", 64), strings.Repeat("b", 64))
						} else if strings.Contains(req.URL.Path, "/blobs/") {
							// Keep the first image file incomplete until cancellation.
							w.Header().Set("Content-Length", "4096")
							w.WriteHeader(http.StatusOK)
							blobOnce.Do(func() { close(blobRequested) })
							w.Write([]byte("{"))
							w.(http.Flusher).Flush()
							select {
							case <-req.Context().Done():
							case <-ctx.Done():
							}
						} else {
							w.WriteHeader(http.StatusOK)
						}
					}))
					defer server.Close()
					args = []string{"clone", "--insecure", strings.TrimPrefix(server.URL, "http://") + "/qualification/image:latest", r.VM}
				}
				cmd := runner.Run(ctx, "tart", args...)
				if e = cmd.Start(); e != nil {
					t.Fatal(e)
				}
				done := make(chan error, 1)
				go func() { done <- cmd.Wait(); close(done) }()
				defer func() {
					cancel()
					select {
					case <-done:
					case <-time.After(5 * time.Second):
					}
				}()
				tmp := filepath.Join(d.config.Build.StateDir, "vms", "tmp")
				deadline := time.Now().Add(10 * time.Second)
				var entries []os.DirEntry
				for time.Now().Before(deadline) {
					entries, e = os.ReadDir(tmp)
					if e != nil {
						t.Fatal(e)
					}
					if len(entries) == 1 {
						break
					}
					select {
					case e := <-done:
						t.Fatalf("clone exited before staging: %v", e)
					default:
					}
					time.Sleep(20 * time.Millisecond)
				}
				if len(entries) != 1 {
					t.Fatal("clone staging checkpoint not reached")
				}
				if operation == "pull" {
					select {
					case <-blobRequested:
					case <-ctx.Done():
						t.Fatal("pull never requested its image file")
					}
				}
				if fault == "cancel" {
					cancel()
				} else {
					d.build.lifeWriter.Close()
				}
				select {
				case e := <-done:
					if e == nil {
						t.Fatal("interrupted clone succeeded")
					}
				case <-time.After(5 * time.Second):
					t.Fatal("supervisor did not exit")
				}
				if _, e = os.Stat(filepath.Join(d.config.Build.StateDir, "vms", "vms", r.VM)); !os.IsNotExist(e) {
					t.Fatal("clone was published")
				}
				syscall.Flock(int(home.Fd()), syscall.LOCK_UN)
				// Hold the abandoned directory's lock: upstream GC deliberately skips it,
				// yet list succeeds. Recovery must retain ownership and reject admission.
				partial, e := os.Open(filepath.Join(tmp, entries[0].Name()))
				if e != nil {
					t.Fatal(e)
				}
				defer partial.Close()
				syscall.Flock(int(partial.Fd()), syscall.LOCK_EX)
				// Parent EOF is durable. Create the next driver's liveness channel.
				if fault == "parent-death" {
					d.build.lifeReader.Close()
					d.build.lifeReader, d.build.lifeWriter, e = os.Pipe()
					if e != nil {
						t.Fatal(e)
					}
					defer d.build.lifeReader.Close()
					defer d.build.lifeWriter.Close()
				}
				if e = d.reconcileBuilds(); e == nil {
					t.Fatal("recovery ignored locked staging")
				}
				syscall.Flock(int(partial.Fd()), syscall.LOCK_UN)
				if e = d.reconcileBuilds(); e != nil {
					t.Fatal(e)
				}
				if e = d.verifyTemporaryStaging(); e != nil {
					t.Fatal(e)
				}
				after, e := os.ReadFile(source)
				if e != nil || string(after) != string(before) {
					t.Fatal("source changed", e)
				}
				saved, e := readRecord(d.recordPath(cfg.ID))
				if e != nil || saved.CleanupPending || saved.Phase != "complete" {
					t.Fatalf("incomplete recovery: %+v %v", saved, e)
				}
			})
		}
	}

}
