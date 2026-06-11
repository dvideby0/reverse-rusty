# ADR-077: Tag-dict fingerprint in the recovery handshakes

**Status:** Accepted (2026-06-11)

**Context.** ADR-065 criterion 9 — the deferred ADR-055 hardening. The cluster's
correctness rests on ONE frozen feature space *and* ONE frozen tag space shared by every
node: segments carry resolved `TagId`s in their tag columns, and translog tails carry raw
`(key,value)` tags that re-resolve against the receiver's tag dict. `AdoptDict` has
verified **both** fingerprints since ADR-055 (shipped bytes recomputed server-side,
echoed back, checked client-side). But the six other fingerprint-guarded RPCs —
`FetchSegments`, `RecoverFrom`, `FetchTranslog`, `RetentionLease`, `Fence`, `Unfence` —
carried and verified only the **feature**-dict fingerprint, and the bare-`connect` probe
(`DictFingerprint`) could not attest the tag space at all. A node whose tag dict diverged
after adoption (a stale durable restore, an operator mixing data dirs) could serve or
accept recovery traffic whose tag columns silently mis-filter — invisible to the
unfiltered match path, wrong under any tag filter or rank.

**Decision.** Mirror the feature-dict pattern exactly, on every guarded surface:

- The six request messages gain `uint64 tag_dict_fingerprint`; each handler refuses a
  mismatch with `failed_precondition` naming the tag space — the same two-line guard
  shape as the dict check beside it.
- `DictFingerprintReply` gains `tag_dict_fingerprint`, so a bare `connect` (no adopt)
  verifies BOTH spaces in its existing probe round-trip. `RemoteShard` stores the
  verified `tag_dict_fp` beside `dict_fp` and presents it on every guarded RPC;
  `connect`/`connect_with_security` take the expected tag fingerprint, threaded from the
  coordinator's shared `TagDict` at every internal connect (peer recovery, catch-up,
  handoff).
- `RecoverFrom`'s outbound `FetchSegments` dial presents the recovering node's own
  (already-verified) tag fingerprint, so the source applies the same guard to the pull.

**Version skew is loud, not silent.** On the connect path this falls out structurally: a
pre-ADR-077 server leaves the probe's new field 0 (proto3), which can never equal a real
fingerprint — the client refuses the link instead of skipping the check (the ADR-075
`ranked`-echo principle, satisfied without an extra echo). On the six request fields the
asymmetry is the reverse — a *stale server* ignoring the new request field serves
unverified — which is exactly the pre-ADR-077 status quo for that hop, never new
wrongness: mixed-version meshes are not yet a supported surface (criterion 10 is
pending), and every link is already gated at attach time by the connect/adopt handshakes
both sides do verify.

**Deliberately out of scope.**
- The self-restart checkpoint sidecar keeps its dict-only fingerprint: a durable node's
  tag space is guarded on disk by ADR-072's `tagdict.bin` persistence + the
  refuse-divergent-disk-state check at `AdoptDict`, so a sidecar field would duplicate an
  existing guard at the cost of a sidecar format bump.
- The ADR-074 tag-wire boundary stands: `RemoteShard` still refuses pre-resolved
  `tag_ids` on the ingest wire. This ADR is the *prerequisite* ADR-074 names for ever
  relaxing that ("ids are exactly what the tag-dict fingerprint handshake certifies both
  sides agree on") — relaxing it remains future work, not licensed here.

**Why this is safe.** The new checks only ever REFUSE (fail-closed); they cannot drop a
match on a healthy mesh. An untagged mesh's finalized-empty tag dicts produce equal
fingerprints by construction, so every existing deployment passes the new guards
unchanged; the percolate hot path is untouched.

**Proven.** `tests/cluster_grpc_oracle/dict_shipping.rs` —
`grpc_recovery_handshakes_reject_divergent_tag_dict`: the bare-connect refusal (wrong
expectation ≡ stale-server zero), raw wrong-tag-fingerprint `Fence`/`Unfence`/
`RetentionLease`/`FetchTranslog` probes (each `failed_precondition` naming the tag
space), and the correct-pair control — mutation-validated (reverting the server-side
checks fails the test). The happy path of all six guards is exercised by every existing
peer-recovery / failover / no-quiesce / handoff / security oracle, which now flow the tag
fingerprint end-to-end (19/19 green).

**See also:** ADR-055 (the tag space through the cluster + the AdoptDict handshake),
ADR-049 (tags die at the boundary), ADR-036/039/040 (the recovery flows these guards
protect), ADR-072 (the durable-node tag-space guard), ADR-074 (the tag-wire boundary),
ADR-065 (the program).
