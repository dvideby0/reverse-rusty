/// Outcome of ingesting a batch of stored queries. Lets callers see how many
/// queries actually entered the index versus why the rest were dropped, instead
/// of silently losing them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct IngestReport {
    /// Queries successfully compiled into the index.
    pub ingested: usize,
    /// Queries dropped because the DSL string failed to parse.
    pub rejected_parse: usize,
    /// Queries dropped as cost-class D (no required feature / any-of to anchor).
    pub rejected_class_d: usize,
}

/// Outcome of an alias import / learn-and-apply (ADR-060): how many groups switched to active,
/// how many stored queries were recompiled so the change took effect (zero false negatives), and
/// the registry's resulting status counts. Returned by [`Engine::import_alias_synonyms`] /
/// [`Engine::learn_aliases_and_apply`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AliasApplyReport {
    /// Whether this call installed a vocabulary or completed a pending rebuild.
    /// An identical import against fully-current state is a no-op.
    pub applied: bool,
    /// Groups newly switched to active by this call.
    pub activated: usize,
    /// Stored queries recompiled so the change took effect immediately (zero false negatives).
    pub recompiled: usize,
    /// The registry's status counts after applying.
    pub summary: crate::vocab::AliasSummary,
}

/// Outcome of applying match-feedback validation to the registry (ADR-103).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AliasFeedbackApplyReport {
    /// Entries stamped with [`FeedbackEvidence`](crate::vocab::FeedbackEvidence).
    pub stamped: usize,
    /// Entries promoted to `Active` (only with `activate=true`; rejected/mixed-kind refused).
    pub activated: usize,
    /// Stored queries recompiled (0 unless something activated — the metadata-only path).
    pub recompiled: usize,
    /// The registry's status counts after applying.
    pub summary: crate::vocab::AliasSummary,
}

/// Outcome of a distributional alias discovery run recorded into the registry (ADR-102).
/// Nothing is ever activated by this path — candidates only — so unlike
/// [`AliasApplyReport`] there is no `activated`/`recompiled` (the install is metadata-only;
/// match results are byte-identical before/after).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AliasDiscoveryReport {
    /// Pairs the discoverer proposed (post-filter, pre-registry).
    pub proposed: usize,
    /// Proposals recorded as NEW `Candidate` entries.
    pub new_candidates: usize,
    /// Proposals that already existed (confidence refreshed, status untouched).
    pub rediscovered: usize,
    /// Proposals refused because the group was operator-`Rejected` (stickiness).
    pub rejected_sticky: usize,
    /// The registry's status counts after recording.
    pub summary: crate::vocab::AliasSummary,
}

/// Outcome of a single live insert. Distinguishes a successful insert (with its
/// memtable-local id) from a class-D rejection. A parse failure is surfaced as
/// `Err(ParseError)` by [`Engine::try_insert_live`], never folded in here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum InsertOutcome {
    /// Inserted; carries the memtable-local id (for a later `tombstone`).
    Inserted(u32),
    /// Compiled but rejected as cost-class D — not stored.
    RejectedClassD,
}

/// Outcome of an atomic upsert (replace-by-id, ADR-067). Distinguishes a fresh
/// registration from a replacement so the HTTP layer can answer ES-style
/// (201-created vs 200-updated). A parse failure is surfaced as
/// `Err(ParseError)` by [`Engine::try_upsert_live`], never folded in here.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpsertOutcome {
    /// No prior live copy existed; inserted fresh. Carries the memtable-local id.
    Created(u32),
    /// Inserted the new version and tombstoned `replaced` prior live copies in
    /// the same critical section (one WAL frame, one snapshot publish).
    Updated { local: u32, replaced: usize },
    /// The NEW version compiled to cost-class D and was rejected — the prior
    /// live copies are left untouched (a failed replace never deletes, matching
    /// ES `index` semantics where a failed op leaves the old document).
    RejectedClassD,
}

/// Per-item outcome for one query in a bulk batch, returned in submission order
/// by [`Engine::try_bulk_ingest_detailed`]. Lets a caller (e.g. the HTTP
/// `/_bulk` handler) report exactly which items were rejected and why — ES-style
/// per-item status — rather than only an aggregate count that hides *which*
/// queries were dropped. The variant tallies match the aggregate [`IngestReport`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngestItemStatus {
    /// Compiled and stored in the new base segment.
    Ingested,
    /// The DSL string failed to parse; carries the diagnostic so the caller can
    /// echo the same detail the single-doc path returns.
    RejectedParse(crate::error::ParseError),
    /// Compiled but rejected as cost-class D — no anchorable feature, not stored.
    RejectedClassD,
}

/// Result of a compaction operation. Tells callers what happened so they can
/// log it, tune the policy, or feed it to telemetry.
#[derive(Debug, Clone, Copy, Default)]
pub struct CompactionReport {
    /// Number of source segments that were merged.
    pub segments_merged: usize,
    /// Total entries in the source segments (alive + dead).
    pub entries_before: usize,
    /// Alive entries in the output segment (dead entries dropped).
    pub entries_after: usize,
    /// Number of tombstoned entries reclaimed.
    pub tombstones_reclaimed: usize,
    /// Number of queries whose signature cover was re-anchored during the merge
    /// (ADR-056). Always `0` unless `compaction_reanchor` is enabled, and `0` in a
    /// cluster shard (frozen dict ⇒ no frequency drift ⇒ no anchor change).
    pub reanchored: usize,
    /// Hot-tier lane moves main→hot during the merge (ADR-105) — `0` unless both
    /// `compaction_reanchor` and `hot_anchor_threshold` are enabled.
    pub hot_promoted: usize,
    /// Hot-tier lane moves hot→main (the θ/2 margin gate passed) during the merge.
    pub hot_demoted: usize,
}
