package driver

import (
	"context"
	"fmt"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
)

func freeDisk(path string) (uint64, error) {
	usage, err := disk.Usage(path)
	if err != nil {
		return 0, fmt.Errorf("checking build storage: %w", err)
	}
	return usage.Free, nil
}

func (c BuildConfig) checkDisk(available uint64, download bool) error {
	needed := uint64(c.MinFreeDiskMB) * 1024 * 1024
	if download {
		// The operator binds this to the pinned image's uncompressed disk size.
		// A cached image needs only the reserve for clone writes and build output.
		needed += uint64(c.ImageDiskMB) * 1024 * 1024
	}
	if available < needed {
		return fmt.Errorf("insufficient build storage: %d bytes available, %d required", available, needed)
	}
	return nil
}

// A runtime reserve complements admission: other host activity and guest writes
// can consume storage after a build starts. Cancellation uses the normal owned
// VM cleanup path; it never prunes another image or deletes host files.
func monitorDisk(ctx context.Context, interval time.Duration, reserve uint64, read func() (uint64, error)) error {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			free, err := read()
			if err != nil {
				return fmt.Errorf("monitoring build storage: %w", err)
			}
			if free < reserve {
				return fmt.Errorf("build canceled to preserve disk reserve: %d bytes free, %d required", free, reserve)
			}
		}
	}
}
