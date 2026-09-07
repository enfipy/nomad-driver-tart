package driver

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	pstructs "github.com/hashicorp/nomad/plugins/shared/structs"
)

type buildRecord struct {
	Schema                     int
	Reattach                   *pstructs.ReattachConfig
	TaskID, AllocID, VM, Image string
	Started                    time.Time
	Finished                   time.Time
	Phase                      string
	ExitCode                   int
	Failure                    string
	CleanupPending             bool
	Artifact                   string
	ArtifactSHA256             string
}

func taskKey(id string) string { sum := sha256.Sum256([]byte(id)); return hex.EncodeToString(sum[:]) }
func newRecord(id, alloc, image string) buildRecord {
	var nonce [8]byte
	if _, e := rand.Read(nonce[:]); e != nil {
		panic(e)
	}
	return buildRecord{Schema: 1, TaskID: id, AllocID: alloc, VM: "cloud-" + taskKey(id)[:24] + "-" + hex.EncodeToString(nonce[:]), Image: image, Started: time.Now(), Phase: "preparing", ExitCode: -1, CleanupPending: true}
}
func (r buildRecord) validate(key string) error {
	if r.Schema != 1 || key != taskKey(r.TaskID) || !strings.HasPrefix(r.VM, "cloud-"+key[:24]+"-") || len(r.VM) != 47 || !nameToken.MatchString(r.VM) || !digestImage.MatchString(r.Image) {
		return fmt.Errorf("invalid owned VM record")
	}
	return nil
}
func safeDir(path string) error {
	if !filepath.IsAbs(path) || filepath.Clean(path) != path {
		return fmt.Errorf("canonical absolute directory required")
	}
	current := "/"
	for _, part := range strings.Split(strings.TrimPrefix(path, "/"), "/") {
		current = filepath.Join(current, part)
		s, e := os.Lstat(current)
		if e != nil {
			return e
		}
		if !s.IsDir() || s.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("unsafe directory: %s", current)
		}
	}
	return nil
}
func privateDir(path string) error {
	if e := safeDir(path); e != nil {
		return e
	}
	s, e := os.Stat(path)
	if e != nil {
		return e
	}
	if s.Mode().Perm()&0077 != 0 || int(s.Sys().(*syscall.Stat_t).Uid) != os.Geteuid() {
		return fmt.Errorf("state must be private and owned by service UID")
	}
	return nil
}
func openRegular(path string, max int64) (*os.File, error) {
	if e := safeDir(filepath.Dir(path)); e != nil {
		return nil, e
	}
	fd, e := syscall.Open(path, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK, 0)
	if e != nil {
		return nil, e
	}
	f := os.NewFile(uintptr(fd), path)
	s, e := f.Stat()
	if e != nil || !s.Mode().IsRegular() || s.Size() > max {
		f.Close()
		return nil, fmt.Errorf("bounded regular file required")
	}
	return f, nil
}
func writeAtomic(path string, v any) error {
	b, e := json.Marshal(v)
	if e != nil {
		return e
	}
	f, e := os.CreateTemp(filepath.Dir(path), ".record-")
	if e != nil {
		return e
	}
	defer os.Remove(f.Name())
	defer f.Close()
	if _, e = f.Write(b); e != nil {
		return e
	}
	if e = f.Sync(); e != nil {
		return e
	}
	if e = f.Close(); e != nil {
		return e
	}
	if e = os.Rename(f.Name(), path); e != nil {
		return e
	}
	d, e := os.Open(filepath.Dir(path))
	if e != nil {
		return e
	}
	defer d.Close()
	return d.Sync()
}
func (d *Driver) recordPath(id string) string {
	return filepath.Join(d.config.Build.StateDir, "records", taskKey(id)+".json")
}
func (d *Driver) save(r buildRecord) error { return writeAtomic(d.recordPath(r.TaskID), r) }
func readRecord(path string) (buildRecord, error) {
	var r buildRecord
	f, e := openRegular(path, 65536)
	if e != nil {
		return r, e
	}
	defer f.Close()
	decoder := json.NewDecoder(f)
	decoder.DisallowUnknownFields()
	if e = decoder.Decode(&r); e != nil {
		return r, e
	}
	return r, r.validate(strings.TrimSuffix(filepath.Base(path), ".json"))
}
