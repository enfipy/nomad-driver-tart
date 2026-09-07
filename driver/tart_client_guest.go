package driver

import (
	"context"
	"errors"
	"fmt"
	"os/exec"
	"time"
)

// Guest-agent RPC is an additional execution backend. Existing SSH execution
// and interactive SSH tasks remain available without a restrictive build profile.
func (c *tartCLI) execGuestAgent(ctx context.Context, config VMConfig, opts ExecOptions) (int, error) {
	if opts.Tty {
		return -1, fmt.Errorf("Tart guest-agent TTY is unsupported")
	}
	args := []string{"exec"}
	if opts.Stdin != nil {
		args = append(args, "-i")
	}
	args = append(args, config.name())
	args = append(args, opts.Command...)
	cmd := c.runner.Run(ctx, "tart", args...)
	cmd.WaitDelay = 5 * time.Second
	cmd.Stdin = opts.Stdin
	cmd.Stdout = opts.Stdout
	cmd.Stderr = opts.Stderr
	if e := cmd.Run(); e != nil {
		var exit *exec.ExitError
		if errors.As(e, &exit) && ctx.Err() == nil {
			return exit.ExitCode(), nil
		}
		return -1, fmt.Errorf("guest execution interrupted: %w", e)
	}
	return 0, nil
}
