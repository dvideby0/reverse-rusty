# ADR-111: Typed ranked wire error codes

- **Status:** Accepted
- **Date:** 2026-07-18

## Context

The ADR-110 ranked RPCs (`PercolateTopK`, `FetchMatches`) carry failures as tonic `Status`. The
client's `ranked_rpc_err` reconstructed the typed `ShardError`s the coordinator's no-partial
contract branches on — enrichment limit (413), ownership/placement mismatch (503), per-id source
loss — by matching the gRPC code **plus substrings of the server's message text** (`read_status`'s
frozen strings, plus two client-side constructions in the fetch-stream drain). That three-string
coupling was flagged in the ADR-107..110 dual review: renaming any message silently breaks
reconstruction, and any `failed_precondition` whose message merely contains "ownership" (for
example a `Protocol` attestation detail) is retyped into a phantom `OwnershipMismatch`.

## Decision

Carry the error class as explicit `Status` metadata; keep the messages frozen as the version-skew
fallback.

- **Module:** `src/cluster/ranked_wire.rs` (`distributed`-gated). ASCII metadata keys
  `rr-ranked-error` (a compact code: `source_unavailable` | `enrichment_limit` |
  `ownership_mismatch`) and `rr-ranked-error-arg` (a decimal `u64` argument: the missing logical id,
  or the group's full byte credit). Keys are deliberately not `grpc-*` (tonic sanitizes those).
- **Producers** attach the code alongside the *unchanged* message: `read_status`'s
  `OwnershipMismatch` / `SourceUnavailable` / `EnrichmentLimit` arms, both
  `validate_placement_config` refusals, and the two client-side fetch-drain constructions
  (placement drift, over-stream credit). `Protocol` deliberately carries no code — its
  `failed_precondition` must not be reconstructed as an ownership failure by an up-to-date peer.
- **Consumer:** `ranked_rpc_err` parses metadata first and falls back to the pre-ADR-111
  code+substring ladder when the code is absent or unusable. A garbled/unknown code degrades to the
  old behavior, never to a failed reconstruction. `SourceUnavailable` without its id argument is
  refused (the substring parser recovers the real id); `EnrichmentLimit` without an argument keeps
  the previously fabricated `limit: 0`.
- **Deadline and admission stay untyped here.** Deadline is already typed by the gRPC status code
  alone. Typing admission would change coordinator behavior (a remote admission failure surfaces as
  a 502 delivery error today, not a caller 400) — out of scope for a behavior-preserving change and
  recorded as a possible follow-up.

## Consequences

- Skew matrix: new server + old client → the client ignores metadata and the frozen messages still
  match; old server + new client → no metadata, the fallback ladder fires. Therefore **the legacy
  message strings are a compatibility contract**: `read_status` and the fetch-drain constructions
  must never reword them (`legacy_rpc_err_preserves_messages_ranked_seam_reconstructs` pins them,
  and `ranked_seam_prefers_metadata_over_message_substrings` pins the metadata-first path).
- Metadata loss by an intermediary is undetectable end-to-end by design (the fallback masks it);
  the producer unit pins are the guard.
- New-peer reconstruction now also carries the true enrichment limit instead of a fabricated zero;
  the coordinator rewrites the limit before it reaches any HTTP surface, so no pinned response
  changes.
