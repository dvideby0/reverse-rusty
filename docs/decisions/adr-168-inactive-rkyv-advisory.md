# ADR-168: Qualify an inactive `rkyv` lockfile advisory

> [Engine, errors, dependencies & ops decisions](areas/engine-quality-and-operations.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

The core CI lane runs both `cargo audit` and `cargo deny --all-features check`. On 2026-08-05 its
unchanged lockfile began failing `cargo audit` for
[`RUSTSEC-2026-0235`](https://rustsec.org/advisories/RUSTSEC-2026-0235.html), an out-of-bounds-read
bug in `rkyv` archive validation. The lock path is `openraft -> byte-unit -> rust_decimal -> rkyv`.
OpenRaft uses byte-unit formatting; byte-unit enables rust_decimal without rust_decimal's defaults;
rust_decimal declares `rkyv` as a separate optional feature. Consequently Cargo resolves `rkyv`
into `Cargo.lock`, but `cargo tree --workspace --all-features --target all` proves that no workspace
target can activate it. Neither the service nor its build and test tools compile, link, deserialize,
or process an `rkyv` archive.

The maintained `rkyv` fix is in the incompatible 0.8 line. The upstream 0.7 declaration belongs to
rust_decimal rather than Reverse Rusty, and patching an unused optional integration locally would
add source ownership and supply-chain risk without changing a shipped artifact. Removing the
lockfile-wide audit would hide real dormant edges; silently ignoring the advisory without checking
feature activation could let a later dependency change make the vulnerability reachable.

## Decision

- Keep `cargo audit` as the lockfile-wide RustSec gate and ignore only `RUSTSEC-2026-0235` on its
  invocation. Do not add a general advisory allowlist or weaken `cargo deny`.
- Immediately follow the scan with a complete
  `cargo tree --workspace --all-features --target all` guard. Fail if any `rkyv` version appears in
  that active graph. This intentionally blocks even a patched `rkyv`: adding the crate to a build
  must first remove or explicitly revisit the broad advisory exception.
- Keep `engine/check.sh` as the canonical command so local pre-push and CI execute the scan and
  activation proof together. Document the qualified exception in the threat model rather than
  suggesting that a bare `cargo audit` is the project gate.
- Remove the exception and guard when the transitive declaration disappears or resolves only
  advisory-free versions. Reassess immediately if OpenRaft, byte-unit, rust_decimal, Cargo feature
  resolution, or any workspace feature changes the graph. The removal work is tracked in the
  [roadmap](../roadmap.md#remove-the-inactive-rkyv-07-lockfile-edge).

## Consequences

The gate remains fail-closed for every compiled dependency and every other package in the lockfile.
It no longer blocks releases on code that cannot enter any workspace artifact, while making future
activation of `rkyv` a hard failure with an actionable message. Developers running bare
`cargo audit` will still see the lockfile finding; `./check.sh --lane core` is the complete policy
decision because it couples the narrowly ignored scan with the activation proof.

This is a reachability qualification, not a claim that affected `rkyv` releases are safe. If the
crate becomes reachable, the only acceptable outcomes are upgrading to a patched line, removing
the dependency path, or recording a new security decision after review; the current exception
cannot carry forward automatically.

## Proof

The graph guard scans normal, build, and development edges for every workspace feature and target.
Its negative case passes on the current lockfile even though `cargo audit` reports the resolved
optional package. The anchored row check makes any active `rkyv` package fail with a remediation
message. The full core lane proves the narrowly ignored RustSec scan, activation guard, all-feature
cargo-deny policy, test suite, independent oracle boundary, and crash-injection suite together.
