package driver

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/shirou/gopsutil/v3/process"
	"golang.org/x/sys/unix"
)

func kernelProcessPath(pid int32) (string, error) {
	// kern.procargs2 begins with argc then the executable path. Do not parse or
	// return argv/environment (which may contain secrets or spoof argv[0]).
	b, err := unix.SysctlRaw("kern.procargs2", int(pid))
	if err != nil {
		return "", err
	}
	if len(b) <= 4 {
		return "", fmt.Errorf("missing executable path")
	}
	end := bytes.IndexByte(b[4:], 0)
	if end <= 0 {
		return "", fmt.Errorf("invalid executable path")
	}
	return filepath.EvalSymlinks(string(b[4 : 4+end]))
}

func kinfoIdentity(p unix.KinfoProc) processIdentity {
	return processIdentity{p.Proc.P_pid, p.Proc.P_starttime.Sec*1e9 + int64(p.Proc.P_starttime.Usec)*1e3}
}

func readBuildProcesses(ctx context.Context, pid int32, path string, started time.Time) ([]buildProcess, error) {
	entries, err := unix.SysctlKinfoProcSlice("kern.proc.uid", os.Geteuid())
	if err != nil {
		return nil, err
	}
	all := make([]buildProcess, 0, len(entries))
	for _, p := range entries {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if p.Proc.P_stat == 5 {
			continue
		} // SZOMB
		r := buildProcess{ID: kinfoIdentity(p), Parent: p.Eproc.Ppid, UID: p.Eproc.Ucred.Uid}
		name := string(bytes.TrimRight(p.Proc.P_comm[:], "\x00"))
		if r.ID.PID == pid || strings.HasPrefix(name, "com.apple.Virtu") {
			r.Path, err = kernelProcessPath(r.ID.PID)
			if err != nil {
				return nil, err
			}
		}
		all = append(all, r)
	}
	realPath, err := filepath.EvalSymlinks(path)
	if err != nil {
		return nil, err
	}
	selected, err := selectBuildProcesses(pid, realPath, started, all)
	if err != nil {
		return nil, err
	}
	for i := range selected {
		p := &selected[i]
		proc, err := process.NewProcessWithContext(ctx, p.ID.PID)
		if err != nil {
			return nil, err
		}
		mem, err := proc.MemoryInfoWithContext(ctx)
		if err != nil {
			return nil, err
		}
		cpu, err := proc.TimesWithContext(ctx)
		if err != nil {
			return nil, err
		}
		again, err := unix.SysctlKinfoProc("kern.proc.pid", int(p.ID.PID))
		if err != nil || again == nil || kinfoIdentity(*again) != p.ID || again.Eproc.Ucred.Uid != p.UID {
			return nil, fmt.Errorf("process changed during resource sample")
		}
		if i < 2 {
			path, err := kernelProcessPath(p.ID.PID)
			if err != nil || path != p.Path {
				return nil, fmt.Errorf("VM executable changed")
			}
		}
		p.RSS, p.CPUSeconds = mem.RSS, cpu.Total()
		// Darwin's no-cgo gopsutil Swap is a page-in counter, not swap bytes.
	}
	return selected, nil
}
