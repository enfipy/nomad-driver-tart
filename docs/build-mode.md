# Operator-controlled disposable builds

The optional `build` block in driver configuration restricts the entire Nomad
client. Without it, upstream VM configuration, SSH execution, networking, mounts,
registry authentication, disk options and prewarm jobs remain available. All
upstream tests are retained. Both modes share `Driver`, `taskHandle`, the Tart
`Client` interface, Nomad executor and completion path.

Cloud-ctrl generates and signs the profile. Required fields inside `build`:

| Fields | Contract |
|---|---|
| `tart_path`, `softnet_dir` | Absolute, root-owned immutable host executables. |
| `state_dir` | Private service-owned directory, independent of personal Tart stores. |
| `image`, `xcode` | Immutable image digest and expected Xcode version; declarations are not proof. |
| `cpu_mhz`, `vcpus`, `memory_mb`, `overhead_mb` | Complete schedulable MHz budget, guest size and host overhead. Task reserves exactly MHz and memory+overhead; no CPU cores or memory oversubscription. |
| `image_disk_mb`, `min_free_disk_mb` | Operator-attested uncompressed image size and free-space reserve (MiB). Admission adds the image budget only for an uncached pull. A one-second free-space monitor cancels through normal cleanup below the reserve; this is not a hard storage quota. Automatic pruning is disabled. |
| `timeout_seconds`, `artifact_mb` | Whole-build deadline and bounded artifact archive. Guest logs are limited to 64 MiB per stream. |
| `network_allow`, `network_block` | Operator IPv4 CIDRs. Blocks must include `0.0.0.0/0` and `@host`; no port exposure. |
| `qualification_jobs` | Explicit job IDs accepted only in `canary`. Submit ACLs must be trusted-operator-only. |

Build tasks supply `command`, `args`, optionally `source` and `artifacts`.
Admission rejects URL/SSH/registry credentials, display, disk, prewarm, mounts,
network/port and task-user overrides before starting anything. Task environment
never reaches host commands or guests. The dedicated Nomad client must additionally
use `driver.allowlist = "tart"`, TLS/ACLs and disabled remote exec. No arbitrary
agent or signing capability is advertised.

Tart guest-agent RPC is an additional command backend, not a replacement for SSH.
A build waits for readiness, optionally imports `local/source.tar` (at most 1 GiB)
into `/tmp/cloud-build`, runs argv once and captures its exit code. With `artifacts`,
it exports `/tmp/cloud-artifacts` as a bounded opaque tar into task-local storage,
with SHA-256 and `build-result.json`. Nothing is extracted on the host. Download
artifacts via authorized allocation filesystem access before Nomad GC. Guest
stdout/stderr are cancellable and bounded; VM startup diagnostics remain in
Nomad task logs.

Ownership is persisted before clone. Short-lived CLI helpers retain the private
store lock and are supervised through a parent-liveness pipe. The existing Nomad
executor supervises the long-lived VM process. Build completion/cancellation joins
both, verifies exact owned-VM deletion and retains failed cleanup for retry. Tart
staging ancestry is checked before GC and must be empty before recovery completes. A
restart first reattaches/stops recorded build executors and cleans owned VMs, then
reports interrupted builds as failed. Ordinary VM/prewarm recovery reattaches its
executor without replaying setup or startup commands. Old handles without recorded
executor identity are rejected; drain old jobs before upgrading.

Export ownership is journaled before creating the partial archive. After stopping
old helpers and the VM, recovery removes the exact partial file and any archive
renamed before its completion metadata was saved. Committed archives and unrelated
files remain untouched. Unexpected file types, ownership or links keep cleanup
pending and block admission, including when another build finishes. The temporary
directory field is omitted after successful cleanup; rollback to older readers
requires a completed drain with no pending cleanup.

Softnet uses longest-prefix rules. Block host public, tailnet, private, link-local
and administration addresses more specifically than any egress allow rule. The
cloud profile allows no guest networking by default. Image/toolchain setup, allowed
egress and software entitlements are operator responsibilities. Never bake signing,
Apple ID or reusable control credentials into an image.

CPU reservations and guest dimensions are not hard host CPU-time/process-memory
quotas. Reported build CPU/RSS includes Tart, its descendants and the attributed
Virtualization.framework VM helper. This requires the profile's dedicated UID and
one active VM. The driver checks the owned VM inventory, kernel executable path,
UID and process start identity. Missing, old or multiple helpers yield no fresh
sample; other users' VMs and the Nomad client are excluded. CPU may exceed 100%
(100% is one host core); RSS is summed host resident memory, not guest-used memory
or a hard memory quota. Swap is not reported because the no-cgo Darwin backend's
field is a page-in counter. Live VM isolation, hardware accounting,
launchd/reboot and signed Xcode/Bevy image readiness remain qualification gates.
Tests use fake Tart, real process/FIFO behavior and Nomad's executor; they do not
establish hypervisor isolation. See [cloud #27](https://github.com/enfipy/cloud/issues/27).

The build profile rejects an all-address `/0` allow rule because Softnet uses it to disable bridge isolation, even alongside a `/0` block. Allow specific destinations and keep private, tailnet, gateway and host-public addresses blocked.
