package driver

import (
	"fmt"
	"net/netip"
	"path/filepath"
	"regexp"
	"slices"

	"github.com/hashicorp/nomad/plugins/drivers"
	"github.com/hashicorp/nomad/plugins/shared/hclspec"
)

// BuildConfig is operator-owned. Tasks cannot override host paths, identity, image,
// network policy or VM size. Initially only named qualification jobs are admitted.
type BuildConfig struct {
	TartPath       string   `codec:"tart_path"`
	SoftnetDir     string   `codec:"softnet_dir"`
	StateDir       string   `codec:"state_dir"`
	Image          string   `codec:"image"`
	Xcode          string   `codec:"xcode"`
	CPU            int64    `codec:"cpu_mhz"`
	VCPUs          int      `codec:"vcpus"`
	MemoryMB       int64    `codec:"memory_mb"`
	OverheadMB     int64    `codec:"overhead_mb"`
	TimeoutSeconds int      `codec:"timeout_seconds"`
	ArtifactMB     int64    `codec:"artifact_mb"`
	Allow          []string `codec:"network_allow"`
	Block          []string `codec:"network_block"`
	Jobs           []string `codec:"qualification_jobs"`
}

var digestImage = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9._:/-]*@sha256:[a-f0-9]{64}$`)
var nameToken = regexp.MustCompile(`^[a-zA-Z0-9][a-zA-Z0-9_.-]{0,127}$`)

func (c BuildConfig) validate() error {
	for _, p := range []string{c.TartPath, c.SoftnetDir, c.StateDir} {
		if !filepath.IsAbs(p) || filepath.Clean(p) != p {
			return fmt.Errorf("absolute canonical operator paths required")
		}
	}
	if !digestImage.MatchString(c.Image) || c.VCPUs < 1 || c.VCPUs > 64 || c.CPU < 1 || c.MemoryMB < 4096 || c.MemoryMB > 1048576 || c.OverheadMB < 512 || c.OverheadMB > 65536 || c.TimeoutSeconds < 1 || c.TimeoutSeconds > 86400 || c.ArtifactMB < 1 || c.ArtifactMB > 16384 || len(c.Jobs) == 0 {
		return fmt.Errorf("invalid pinned image, resource profile or qualification allowlist")
	}
	for _, j := range c.Jobs {
		if !nameToken.MatchString(j) {
			return fmt.Errorf("invalid qualification job")
		}
	}
	if !slices.Contains(c.Block, "0.0.0.0/0") || !slices.Contains(c.Block, "@host") {
		return fmt.Errorf("default deny and @host block required")
	}
	for _, list := range [][]string{c.Allow, c.Block} {
		for _, s := range list {
			if s == "@host" && slices.Contains(c.Block, s) {
				continue
			}
			prefix, e := netip.ParsePrefix(s)
			if e != nil || !prefix.Addr().Is4() {
				return fmt.Errorf("IPv4 CIDRs required")
			}
		}
	}
	// An allowlist is intentionally operator-maintained. Include all private,
	// tailnet, link-local and host public addresses in more-specific block rules.
	return nil
}

func (c BuildConfig) validateTask(t *drivers.TaskConfig, tc TaskConfig) error {
	if t.Namespace != "canary" || !slices.Contains(c.Jobs, t.JobID) {
		return fmt.Errorf("only operator-allowlisted canary jobs are enabled; untrusted execution is unqualified")
	}
	if tc.URL != "" || tc.SSHUser != "" || tc.SSHPassword != "" || tc.Auth.Username != "" || tc.Auth.Password != "" || tc.ShowUI || tc.DiskSize != 0 || tc.PullOnly || tc.Network != nil || tc.RootDisk != nil || len(tc.Directories) > 0 || len(nomadPortExposures(t)) > 0 {
		return fmt.Errorf("build profile forbids image, SSH, registry auth, display, disk, prewarm, mounts and network overrides")
	}
	if t.User != "" {
		return fmt.Errorf("task user overrides are forbidden")
	}
	if tc.Command == "" || len(tc.Args) > 128 {
		return fmt.Errorf("guest command required")
	}
	r := t.Resources
	if r == nil || r.NomadResources == nil || r.NomadResources.Cpu.CpuShares != c.CPU || r.NomadResources.Memory.MemoryMB != c.MemoryMB+c.OverheadMB || r.NomadResources.Memory.MemoryMaxMB > r.NomadResources.Memory.MemoryMB || len(r.NomadResources.Cpu.ReservedCores) != 0 {
		return fmt.Errorf("task must reserve the complete CPU budget and guest RAM plus overhead; no cores or oversubscription")
	}
	return nil
}

var buildConfigSpec = hclspec.NewObject(map[string]*hclspec.Spec{
	"tart_path":          hclspec.NewAttr("tart_path", "string", true),
	"softnet_dir":        hclspec.NewAttr("softnet_dir", "string", true),
	"state_dir":          hclspec.NewAttr("state_dir", "string", true),
	"image":              hclspec.NewAttr("image", "string", true),
	"xcode":              hclspec.NewAttr("xcode", "string", true),
	"cpu_mhz":            hclspec.NewAttr("cpu_mhz", "number", true),
	"vcpus":              hclspec.NewAttr("vcpus", "number", true),
	"memory_mb":          hclspec.NewAttr("memory_mb", "number", true),
	"overhead_mb":        hclspec.NewAttr("overhead_mb", "number", true),
	"timeout_seconds":    hclspec.NewAttr("timeout_seconds", "number", true),
	"artifact_mb":        hclspec.NewAttr("artifact_mb", "number", true),
	"network_allow":      hclspec.NewAttr("network_allow", "list(string)", true),
	"network_block":      hclspec.NewAttr("network_block", "list(string)", true),
	"qualification_jobs": hclspec.NewAttr("qualification_jobs", "list(string)", true),
})
