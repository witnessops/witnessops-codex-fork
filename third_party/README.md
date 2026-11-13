# Dependency compatibility bridges

These two path patches keep the released Rama 0.3.0-alpha.4 and SQLx 0.9.0 APIs
while allowing fixed Hickory 0.26.2 and libsqlite3-sys 0.39.0 (SQLite 3.54.0).
Rust stays at 1.95.0. The released dependency constraints do not admit these fixes.

- `rama-dns`: published crate sources plus original MIT/Apache notices from its
  recorded source revision. Only `Cargo.toml` and `src/hickory.rs` differ: fixed
  Hickory version, its runtime/configuration API, fallible resolver construction,
  and typed extraction from generic DNS answers. The existing infallible builder
  remains available; `try_build` exposes configuration errors.
- `sqlx-sqlite`: published crate sources, with packaged relative license references
  resolved to the original notices at the recorded source revision. Only the `libsqlite3-sys`
  upper bound changes from `<0.38.0` to `<0.40.0`, matching
  [SQLx upstream's accepted bound](https://github.com/launchbadge/sqlx/commit/10462fa4414c6c7f1ed1d39bb31678e3b726878c).
  No driver behavior changes are included. Three upstream documentation lines
  have trailing whitespace removed to satisfy the existing Git diff check.

`PROVENANCE.json` records the registry archive checksums, original Git revisions,
modified-file lists and hashes of every vendored file. Cache metadata, packaged
lockfiles and original unnormalized manifests are omitted. Preserve these source
copies and their notices; house formatting and spelling checks exclude only the
two named upstream directories. The adapter is formatted separately.

Compatibility coverage lives in `codex-rs/network-proxy/tests/dns_compatibility.rs`
and the SQLite tests under `codex-rs/state`. It covers loopback A/AAAA/TXT and
negative DNS answers, malformed SVCB ALPN rejection, the fixed SQLite runtime,
and committed WAL persistence. It does not establish full cross-platform or
MSVC validation.

Remove each bridge, its path patch and formatter exclusions when a compatible
published upstream release admits the fixed dependency and passes the focused
checks. Regenerate both Cargo and Bazel lock metadata together. Until removal,
maintain these copies as downstream code; do not treat them as unchanged upstream
releases or add advisory waivers to accommodate them.
