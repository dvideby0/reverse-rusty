# ADR-097: Content-fingerprint skip — provably-complete retained members keep their data

**Status:** Accepted (2026-07-02)

**Context.** The ADR-094 group move re-establishes every RETAINED member (`R = (D ∩ C) ∖ {cp}`)
from the fenced, frozen source via an unconditional `RecoverFrom` — an `O(corpus)` copy per member
**inside the fence window** — because completeness had no cheaper proof: replica in-sync state is
composite-private (deliberately never read by the coordinator), translog positions are
per-shard-instance local (a member has no "applied through source position X" cursor), and
byte-level segment CRCs cannot compare copies (logically-equal replicas are byte-divergent by
construction — their own flush/compaction boundaries). So a **pure promotion** — swap primary and
replica, both members retained and almost certainly already identical — paid a full re-copy.
ADR-094 recorded the deferral ("an in-sync-snapshot + content-fingerprint protocol to skip
provably-complete members").

**Decision.** A **logical content fingerprint**, compared where both sides are provably
quiescent. All `distributed`-gated; behavior on divergent content is byte-identical to ADR-094.

- **`LocalShard::content_fingerprint128`**: an order-independent 128-bit hash + live count over
  the shard's live query multiset — `(logical_id, version, dsl, TagId*)`, exactly the
  `live_sources_tagged` basis (memtable + segments, live copies only — the same enumeration the
  vocabulary rebuild trusts for completeness). Each entry is canonically encoded (LE scalars,
  length-prefixed dsl/tags), the encodings sorted (the multiset canon — no `Ord` on entry types
  needed), then folded through two independently-seeded FNV-1a streams. Insertion order, flush
  boundaries, segment layout, and compaction history cannot change it; a version, tag, or
  live-set change must.
- **`ContentFingerprint` RPC** (additive; guarded by the dict/tag fingerprints like every
  recovery RPC; deliberately fence-transparent — the caller's whole point is asking while the
  group is write-quiesced). An old peer answers `Unimplemented`.
- **The phase-6 skip** (`reassign_group_and_move`): compute the frozen source's fingerprint ONCE
  after the freeze-probe; for each retained member (post `clear_stale_fence`), equal fingerprint
  ⇒ **skip the `RecoverFrom` + verify catch-up entirely**; anything else — mismatch, RPC error,
  an old peer — falls back to the proven re-copy, so the heal path is unchanged and a fingerprint
  failure costs a redundant copy, never correctness. **Soundness**: at phase 6 the source is
  frozen (post-freeze-probe) and the member is write-quiesced (composite writes are
  primary-first and `cp` is fenced — one fence quiesces the whole group), so equal live multisets
  at that instant mean the member already holds exactly what the re-copy would install.
- **Honest non-coverage** (stated, deliberate): `sources.dat` display divergence and
  translog/segment-layout divergence (neither is on the match path); ~2⁻¹²⁸ collision odds.

**Consequences.** A pure promotion's fence window collapses from `O(corpus)` to freeze-probe +
two fingerprint RPCs + assemble/swap; a mixed move re-copies only the genuinely-desynced members.
This removes most of the value of the other ADR-94 cost deferral — **server-side staged recovery
(shadow install, atomic promote), which stays deferred**: it would move even a desynced member's
copy out of the fence window, but post-ADR-097 that copy runs only on the rare divergent-content
path (~900–1,400 lines of slot state machine + dir-swap crash proofs for a rare case; revisit if
real fence windows still hurt). The fingerprint is also independently useful introspection (a
cheap cross-copy consistency probe).

**Proof.** `cluster_grpc_oracle::fingerprint` — the **pure-promotion skip**
(`recover_from` RPC delta = 0 across the move, fingerprints consulted, ≡ brute live + across a
resolve-only coordinator restart) and the **desync guard-rail** (a replica desynced out-of-band
through a second coordinator fingerprint-mismatches, is re-copied — `recover_from` delta ≥ 1 —
and the rogue entry is healed away: the promoted primary serves exactly the source's live set,
≡ brute). Unit: order/layout independence + memtable inclusion (reverse-order memtable-only copy
≡ flushed copy), version sensitivity, tag sensitivity, delete sensitivity/history-freedom (a
delete restores the smaller set's fingerprint). Full 53-test distributed oracle green
(ADR-094's suite unperturbed — the heal path is byte-identical).
