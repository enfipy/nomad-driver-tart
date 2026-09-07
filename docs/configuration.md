# Operator configuration

Cloud-ctrl generates the authoritative configuration. Required fields:

| Field | Meaning |
|---|---|
| `enabled` | Explicit enablement; initially trusted qualification only. |
| `tart_path`, `softnet_dir` | Absolute paths with root-owned, non-writable ancestry. |
| `state_dir` | Canonical private service-owned directory; dedicated VM store and lock. |
| `image`, `xcode` | Image digest and expected guest toolchain; declarations are not qualification evidence. |
| `cpu_mhz`, `vcpus`, `memory_mb`, `overhead_mb` | Full schedulable CPU budget, guest dimensions and host overhead. Task must reserve exactly MHz and memory+overhead; no `cores` or memory oversubscription. |
| `timeout_seconds`, `artifact_mb` | Whole build deadline and bounded artifact archive. Logs have a fixed 64 MiB limit per stream. |
| `network_allow`, `network_block` | Operator IPv4 CIDRs. Blocks must include `0.0.0.0/0` and `@host`. No exposure/port mapping. |
| `qualification_jobs` | Explicit job IDs accepted only in `canary`. Submission ACL must be trusted-operator-only. |

Softnet chooses the longest prefix (deny wins ties). Block host public, tailnet,
private, link-local and administration addresses more specifically than egress
allows. The default cloud profile allows no guest networking. Prepare an image with
cached build inputs, or explicitly allow only the necessary build endpoints. Runtime
commands receive a clean host environment and never receive task environment values.

After changing this profile, drain and stop Nomad before restart. An identical
SetConfig RPC is idempotent; changing a live profile is rejected. Images need a
working Tart guest agent, Xcode/toolchains and accepted licenses. No registry or
Apple credentials are supplied by task config. Prepare private images through an
operator-controlled process without baking reusable secrets into them.
