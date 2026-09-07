# Nomad Tart driver — cloud qualification fork

Fork of brianmichel/nomad-driver-tart, narrowed to disposable macOS builds for
[cloud #27](https://github.com/enfipy/cloud/issues/27). **Trusted qualification
only; real Tart/Softnet isolation and restart qualification are still required.**

One operator-pinned image/profile per Apple Silicon Mac; one active VM. Nomad
reserves the entire configured CPU MHz budget and guest RAM plus overhead. Guest
vCPU/RAM are set independently. This is not a hard host CPU quota or memory limit.

The task API is `command`, `args`, optional `source` and `artifacts`. All commands
run through Tart guest-agent RPC. No SSH, shared directories, task user, environment
forwarding, host/bridged network, forwarded ports or signing credentials. Logs and
guest exit status go to Nomad. Optional source is `local/source.tar` (at most 1 GiB),
streamed into `/tmp/cloud-build` in the guest. Artifacts from `/tmp/cloud-artifacts`
are exported as an opaque bounded tar to task-local storage, with SHA-256 and
`build-result.json`. Download before allocation GC; no host extraction or durable
object-store upload. Task events expose phase and artifact metadata.

Ownership is journaled before clone. Cancellation and every failure converge on
verified stop/delete. Cleanup failure blocks admission. Plugin restart fails the
interrupted build and cleans its exact VM; it never replays commands. Same-binary
helper supervision retains the store lock until old Tart helpers are gone, including
parent SIGKILL. CPU/RSS samples describe the host Tart process, not guest-used RAM.

Install through [cloud-ctrl](https://github.com/enfipy/cloud). It pins this fork's
commit, Go compiler, binary digest, Tart and Softnet archives. The driver requires
an unprivileged dedicated service account, private state and root-owned immutable
executables. Only Softnet is setuid-root, restricted to the build group. Nomad must
set `driver.allowlist = "tart"`, disable remote exec, enforce TLS/ACLs, and restrict
canary submission to trusted operators. A job name or namespace alone is not trust.

Build/test using Go 1.27.0:

```sh
go test -race ./...
CGO_ENABLED=0 GOOS=darwin GOARCH=arm64 go build -trimpath -buildvcs=false .
```

Tests use fake Tart plus real FIFOs/process groups. They establish driver semantics,
not hypervisor isolation. Live tests must exercise clone/boot/build/export failures,
plugin/Nomad SIGKILL, host reboot, default-deny network, host/keychain access attempts,
resource pressure and unrelated-VM preservation. The older upstream task API is unsupported; historical examples are retained
with an explicit warning. This fork does not provide interactive agents.
