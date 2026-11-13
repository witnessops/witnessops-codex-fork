# WitnessOps native fork recovery

This branch reconstructs existing local work against upstream base
`275a9a89b6a2689b5b5a8b3b4aa5dc3c02e7d64a`. It preserves the Codex workspace and
packaging architecture. It does not install a runtime, change launcher routes,
enable services, or authorize external actions.

## Recovered source and provenance

The native baseline is the cumulative Nov3 complete-client unit-completion
patch, SHA-256
`a70a8cc3ea8217537491eac33d99163566dd9961c4908cd32f21888cacfbad23`.
It changes 126 files (21 new), adding 11,627 lines and removing 200. Its 9,821
regular source files were matched to the frozen source manifest before any
reconciliation edits. The source manifest SHA-256 is
`19ed90ee6df0c37ecfe2b71d1da9c1551fe5f239c1f9dd71832b8574bc3408c1`.

On ops-laptop-03, immutable original evidence lives under:

```text
/home/karol/.local/share/witnessops/laptop/observations/2026-11-03/
  sovereign-complete-client-164739Z/unit-completion/
```

The original `FINAL-RECEIPT.json` hash is
`052f4066ccd5374c07c549aacf44d0fb4ec1ef8d06a6b94212139ee9aa2754db`.
It records a prior offline build and 570 targeted checks, not full-workspace
coverage or newly run reconstruction tests. Original failed/intermediate
candidates and their receipts remain unchanged.

The sibling effective-mode policy comes from the Nov1 Documents lineage:

```text
/home/karol/Documents/Codex/2026-11-01/
  sovereign-codex-effective-mode-enforcement-v1/evidence/SOURCE-ONLY.patch
  sovereign-codex-indirect-dispatch-offline-v1/work/source/
```

The effective-mode patch hash is
`2f4ff02f54e31bcdb5a58410a89bfaf1d2502a43199dd5f783755856d927083f`.
Corrected indirect-test receipt hash:
`1829173a5edea1d5715539b9ea69718687388c9acfd913d18847084a8ec284e8`.
Five files containing the mode requirement, guard, guardian opt-out and core
policy/dispatch tests were recovered from that corrected source. Lifecycle
checks were adapted to the newer explicit owner mode. The complete older
processor and its positive owner-daemon cases were not imported.

The forensic inventory, per-file hashes and conflicting candidates are in
`codex-fork-full-recovery-yDyrkH` under the Nov6 observation directory. New
source-integration and validation evidence is in its sibling
`codex-fork-integration-20261106`. These are host evidence locations, not runtime
configuration or a portable package dependency.

## Behavior preserved and reconciled

Normal Codex sessions remain the default. `--sovereign-read-head` explicitly
selects the restricted owner workflow. That workflow retains its recovered
fixed model (`gpt-5.6-terra`), codex-ops repository path, managed read-only
profile and exact namespaced `witnessops/read_head` tool. These are specialized
owner constraints; they do not apply to ordinary sessions.

The recovered native implementation includes start, cold/warm resume and fork
admission; persisted owner markers; mutation locks; fixed dynamic-tool admission;
CLI/TUI callback wiring; callback identity and cleanup acknowledgement;
in-memory reconnect without replay; process supervision and descendant reaping.
See [callback cleanup](sovereign-callback-cleanup.md) and the
[client contract](../codex-rs/app-server-client/README.md).

The sibling policy adds an immutable `require_code_mode_only` ceiling. Its
default is false. The explicit owner constructor enables it, and each tool-plan
construction fails before returning a router if resolved mode is Direct/hybrid
or the Code Mode host is unavailable. The captured allowlist and sandbox/unified
exec requirements remain in force. Normal sessions and guardian-reviewer retain
their previous mode behavior.

Warm resume checks the immutable Session policy used by routing as well as the
explicit host owner marker and current settings; a mutable extension attachment
cannot substitute a weaker policy. The recovered negative matrix complements
the newer mutation locks. Owner daemon admission remains refused; historical
positive daemon experiments do not define the admitted foreground workflow.

The recovered synthetic tests cover mode metadata precedence, Direct/hybrid/
unavailable-host refusal, Normal defaults, hostile multi-source tool inventory,
17 forbidden tool names through direct and Code Mode dispatch, handler counters
and a permitted positive control. They complement the existing actual-thread
inventory/refusal, persisted history, callback, cancellation and filesystem tests.
They do not contact real connectors or providers.

## Offline reproduction

Use the pinned Rust toolchain in `codex-rs/rust-toolchain.toml` (1.95.0), the
recovered Cargo.lock, locally available dependency caches, and fresh private
target/output directories. Keep network access disabled and Cargo locked/offline.
Do not run old one-shot evidence scripts in their original directories: those
scripts contain path-bound output and consumed claim records.

The new host evidence includes a structured rootless-container runner. It binds
this worktree and original dependency inputs read-only; only new copy-on-write
target/cache layers and synthetic fixtures are writable. It uses a pinned local
image, `--pull=never`, `--network=none`, a read-only root, dropped capabilities,
no-new-privileges, and bounded CPU/memory/process counts. No operator credentials
or real codex-ops checkout are mounted into the test fixture.

Representative commands inside that environment, from `codex-rs`:

```sh
cargo fmt --all -- --check
cargo test --offline --locked --release -p codex-core -p codex-app-server --lib --no-run
cargo test --offline --locked --release -p codex-app-server-client -p codex-utils-cli --lib --no-run
cargo build --offline --locked --release -p codex-cli --bin codex \
  -p codex-code-mode-host --bin codex-code-mode-host \
  -p codex-exec-server-cli --bin codex-exec-server \
  -p codex-app-server --bin codex-app-server
```

Build the Linux sandbox and filesystem helper from this same source for their
tests. Validate Normal and owner lifecycle, effective-mode/dispatch matrices,
cleanup/ACK, persistence, client, CLI and selected TUI paths. Record exact test
names and counts; never equate a filtered suite with all workspace tests. TUI
cases use the recovered isolated-process runner and stack setting.

Use `scripts/build_codex_package.py` with explicit freshly built entrypoint,
Code Mode host, exec-server, bwrap and rg inputs, into a new unselected package
directory. Retain license/helper manifests and hashes. Test any packaged CLI
only against synthetic local fixtures in the network-disabled environment.
Compilation, package verification, installed-runtime selection and live-provider
acceptance are distinct states.

## Project and runtime boundaries

Security CLI owns `codex-wops` staging, selection and the supplied-evidence
snapshot validator. The WitnessOps workspace owns the laptop router. Skills,
wops-mcp, Sovereign Tasks and control-plane experiments retain separate owners.
They are not vendored into this native fork merely because they interact with it.

The pre-existing selected runtime on this host already came from the recovered
Nov3 patched snapshot. This reconstruction branch includes additional policy
reconciliation and is a different source state. Its build or test success does
not update that installed binary. No installation, activation, production use,
push or merge is part of the source recovery authorization.

Reconnect recovery remains in memory and requires the retained original owner
and server generation. Durable process-crash recovery/outboxes remain absent.
Historical scope exclusions and unresolved candidates stay in the forensic
inventory; no original source or evidence is deleted or overwritten.
