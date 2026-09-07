package driver

import (
	"context"
	"errors"
	"io"
	"os"
	"os/exec"
	"os/signal"
	"syscall"
	"time"
)

// Each Tart invocation has a small supervisor in this same signed binary. It
// retains the driver's store lock until its process group is dead, including
// when the plugin is SIGKILLed between clone start and VM publication. A new
// plugin cannot reconcile while an old helper can still mutate the store.
func init() {
	if len(os.Args) < 3 || os.Args[1] != "--cloud-tart-child" {
		return
	}
	life := os.NewFile(3, "parent-life")
	lock := os.NewFile(4, "store-lock")
	defer lock.Close()
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGTERM, syscall.SIGINT)
	cmd := exec.Command(os.Args[2], os.Args[3:]...)
	cmd.Stdin, cmd.Stdout, cmd.Stderr = os.Stdin, os.Stdout, os.Stderr
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	if e := cmd.Start(); e != nil {
		os.Exit(125)
	}
	done := make(chan error, 1)
	go func() { done <- cmd.Wait() }()
	gone := make(chan struct{})
	go func() { _, _ = io.Copy(io.Discard, life); close(gone) }()
	var e error
	select {
	case e = <-done:
	case <-gone:
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		e = <-done
	case <-signals:
		_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
		e = <-done
	}
	// A helper may spawn descendants that outlive the immediate process.
	_ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL)
	if e != nil {
		var exit *exec.ExitError
		if errors.As(e, &exit) && exit.ExitCode() >= 0 {
			os.Exit(exit.ExitCode())
		}
		os.Exit(125)
	}
	os.Exit(0)
}

// Nonblocking FIFO IO keeps cancellation effective even if Nomad's log monitor
// has died or stopped reading. Never create or follow a caller-supplied path.
func openLog(ctx context.Context, path string) (*os.File, error) {
	for {
		fd, e := syscall.Open(path, syscall.O_WRONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
		if e == nil {
			f := os.NewFile(uintptr(fd), path)
			s, err := f.Stat()
			if err != nil || (!s.Mode().IsRegular() && s.Mode()&os.ModeNamedPipe == 0) {
				f.Close()
				return nil, errors.New("regular log or FIFO required")
			}
			return f, nil
		}
		if e != syscall.ENXIO && e != syscall.EINTR {
			return nil, e
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(20 * time.Millisecond):
		}
	}
}

type logWriter struct {
	ctx  context.Context
	file *os.File
}

func (w logWriter) Write(p []byte) (int, error) {
	total := 0
	// Direct syscall avoids Go's poller turning a nonblocking FIFO into an
	// unbounded blocking Write. Cancellation is checked between partial writes.
	for len(p) > 0 {
		if e := w.ctx.Err(); e != nil {
			return total, e
		}
		var n int
		var e error
		raw, err := w.file.SyscallConn()
		if err != nil {
			return total, err
		}
		if err = raw.Control(func(fd uintptr) { n, e = syscall.Write(int(fd), p) }); err != nil {
			return total, err
		}
		if n > 0 {
			total += n
			p = p[n:]
		}
		if e != nil && e != syscall.EAGAIN && e != syscall.EINTR {
			return total, e
		}
		if e != nil {
			select {
			case <-w.ctx.Done():
				return total, w.ctx.Err()
			case <-time.After(20 * time.Millisecond):
			}
		}
	}
	return total, nil
}
