//go:build !darwin

package driver

import (
	"context"
	"fmt"
	"time"
)

func readBuildProcesses(context.Context, int32, string, time.Time) ([]buildProcess, error) {
	return nil, fmt.Errorf("VM process accounting requires macOS")
}
