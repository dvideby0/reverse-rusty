# ADR-163: Distributed ranking-profile attestation

> [Percolator parity decisions](areas/percolator-parity.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

## Context

ADR-162 introduced deterministic, fingerprinted CPU ranking profiles but deliberately restricted
named linear and tree profiles to a single process. The first gRPC rank program carried only static
priority and tag boosts. Silently substituting that static scorer on a remote shard would preserve
Boolean membership while corrupting relevance order, so the coordinator refused rich profiles.

The model interpreter and its strict startup bounds are already part of every binary. The remaining
cross-process problem is identity, not executable code: a coordinator must know that every required
shard resolved the requested name to exactly the same compiled semantics. This must hold for scalar
top-K, batch top-K, and exhaustive delivery, including mixed-version and partially streamed failures.

## Decision

- Operators load the same strict ranking-profile JSON on the coordinator and every `shardserver`.
  Both binaries accept `--ranking-profiles-file` or `RR_RANKING_PROFILES_FILE`. Compose can mount one
  host file with `compose.ranking-profiles.yml`; Helm can mount one operator-owned ConfigMap through
  `rankingProfiles.configMapName`, or a generic shared volume source for registries above the
  ConfigMap object limit.
- Every ranked gRPC request carries the selected profile name and its semantic 64-bit fingerprint
  with the existing compiled priority/tag-boost program. Model bytes are not shipped per request.
  Each shard resolves the identity against its startup registry before scoring. An unknown name or
  fingerprint mismatch is a protocol failure, never a fallback.
- A successful top-K reply and each terminal batch/exhaustive summary echo the resolved identity.
  The coordinator compares that echo before accepting the result. Batch title frames and exhaustive
  chunks remain provisional until their terminal attestation; a missing or different echo fails the
  whole operation.
- An absent request identity is interpreted only as the built-in `static_v1`, preserving old-client
  to new-server bounded reads. A new coordinator requires the reply echo even for `static_v1`, so an
  old shard fails closed. Upgrade shards before the coordinator to avoid a ranked-read outage; any
  other rollout order remains safe because affected requests fail rather than return drifted scores.
- Profile identity is request-scoped rather than a whole-registry connection fence. Nodes may carry
  additional unused profiles; the exact selected profile must agree everywhere it is routed.

## Alternatives considered

- **Ship the complete model in every request.** This removes external configuration coordination but
  repeats bounded-yet-material bytes on every shard fan-out and makes the hot wire a model loader.
- **Attest the complete registry only at connect time.** This rejects harmless extra profiles and
  still needs request identity to pin the selected behavior across later configuration changes.
- **Use only the profile name.** Names are operator-controlled aliases and cannot detect divergent
  weights, trees, or feature configuration.
- **Allow static fallback on mismatch.** Membership would remain correct, but returned ranking would
  violate the caller's explicit contract.

## Consequences

Named linear and tree profiles now work in single-node, in-process cluster, remote Compose, and
remote Helm topologies. Wire overhead is one short identity per shard request and terminal reply;
scoring cost is unchanged. Operational rollout must distribute the same file before selecting a
profile. A bad or incomplete rollout causes explicit request failures rather than score drift.

This decision attests deterministic CPU programs only. It does not distribute training artifacts,
provide dynamic model reload, or define accelerator-backed reranking.

## Safety and proof

The Boolean candidate and exact-verification paths are unchanged; ranking remains post-match and
cannot add or remove members. Proto unit tests pin identity encoding, legacy static decoding, and
unknown/divergent registry rejection. Real gRPC oracle tests run one rich profile through scalar
top-K, batch, and exhaustive delivery, reject a divergent shard fingerprint, and reject a
pre-ADR-163 server that omits the terminal echo. Existing distributed exactness rules continue to
forbid partial success when any required shard or completion summary fails.
