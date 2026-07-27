//! Lock-free snapshot inspection and point reads.
//!
//! This module deliberately owns no candidate traversal or exact verification.
//! Its only cross-boundary seam is newest-live source metadata, which matching
//! uses for compatibility ranking after Boolean truth has already been decided.

use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::segment::{BaseSegment, EngineSnapshot};
use crate::vocab::Vocab;

impl std::fmt::Debug for EngineSnapshot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EngineSnapshot")
            .field("base_segments", &self.segments.len())
            .field("memtable_entries", &self.memtable.len())
            .field("query_store_entries", &self.query_store.len())
            .field("vocab_epoch", &self.vocab_epoch)
            .finish()
    }
}

impl EngineSnapshot {
    pub(crate) fn validate_ownership_for_shard(
        &self,
        position: u32,
        generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
    ) -> Result<(), crate::ownership::OwnershipError> {
        for segment in &self.segments {
            for local in 0..segment.len() as u32 {
                segment
                    .placement(local)
                    .to_owned()
                    .validate_for_shard(position, generation, num_shards)?;
            }
        }
        for local in 0..self.memtable.len() as u32 {
            self.memtable
                .placement(local)
                .to_owned()
                .validate_for_shard(position, generation, num_shards)?;
        }
        Ok(())
    }

    pub fn normalizer(&self) -> &Normalizer {
        &self.norm
    }

    pub fn dict(&self) -> &Dict {
        &self.dict
    }

    /// The vocabulary captured at snapshot time, if one was set. Lets read
    /// endpoints (`GET /_vocab`) serve the vocab from the lock-free snapshot
    /// without locking the engine (ADR-016).
    pub fn vocab(&self) -> Option<&Vocab> {
        self.vocab.as_deref()
    }

    /// The engine configuration captured at snapshot time. Lets `GET /_settings`
    /// serve the live settings from the lock-free snapshot (ADR-016).
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    pub fn num_queries(&self) -> usize {
        self.segments.iter().map(|s| s.len()).sum::<usize>() + self.memtable.len()
    }

    pub fn num_segments(&self) -> usize {
        self.segments.len() + 1
    }

    pub fn rejected_parse(&self) -> u64 {
        self.rejected_parse
    }

    pub fn rejected_class_d(&self) -> u64 {
        self.rejected_class_d
    }

    /// Observe-first hot-tier telemetry (the Broad-Query Cost Program): accepted
    /// compiles since process start whose plan would reclassify to the hot tier
    /// under [`DEFAULT_HOT_ANCHOR_THETA`](crate::config::DEFAULT_HOT_ANCHOR_THETA).
    pub fn would_be_hot(&self) -> u64 {
        self.would_be_hot
    }

    /// Dedup Stage A telemetry (process-lifetime): accepted compile events.
    pub fn bodies_total(&self) -> u64 {
        self.bodies_total
    }

    /// Dedup Stage A telemetry: accepted compiles that joined an existing body
    /// group in their segment (what per-segment sharing actually captured).
    pub fn dup_joined(&self) -> u64 {
        self.dup_joined
    }

    /// Linear-counting estimate of DISTINCT canonical bodies seen since process
    /// start — global duplication, incl. the cross-segment share Stage A's
    /// per-segment groups cannot capture (Stage B sizing evidence). 0 until the
    /// first accepted compile.
    pub fn distinct_bodies_est(&self) -> u64 {
        self.distinct_bodies_est
    }

    pub fn vocab_epoch(&self) -> u64 {
        self.vocab_epoch
    }

    pub fn wal_healthy(&self) -> bool {
        self.wal_healthy
    }

    pub fn persistence_healthy(&self) -> bool {
        self.persistence_healthy
    }

    pub fn skipped_segments(&self) -> usize {
        self.skipped_segments
    }

    pub fn stale_segment_count(&self) -> usize {
        let current = self.vocab_epoch;
        self.segments
            .iter()
            .filter(|s| s.vocab_epoch() < current)
            .count()
            + usize::from(self.memtable.vocab_epoch < current && !self.memtable.is_empty())
    }

    pub fn has_stale_segments(&self) -> bool {
        self.stale_segment_count() > 0
    }

    pub fn get_query_source(&self, logical_id: u64) -> Option<String> {
        self.get_query_source_bounded(logical_id, usize::MAX)
            .ok()
            .flatten()
    }

    /// Canonical stored document for `GET /_doc/{id}`: original DSL text, the
    /// newest live write version, and scalar-coerced metadata tags. Source files
    /// without the ADR-116 metadata footer predate tag read-back; for those, dense tag ids are losslessly
    /// reconstructed through the persisted tag dictionary. A legacy synthetic
    /// tag cannot be reversed and leaves `tags_known = false` so the HTTP layer
    /// can fail loud rather than return incomplete metadata.
    pub fn get_query_document(&self, logical_id: u64) -> Option<crate::storage::StoredSource> {
        let mut source = self.query_store.get_document(logical_id)?;
        let (version, source_generation, tag_ids) = self.source_metadata_for_logical(logical_id)?;
        if source.metadata_known() {
            // SourceStore is shared behind interior mutability, so an older
            // published snapshot can observe a later source mutation. The
            // caller-visible version may repeat; the internal generation cannot.
            // A durable sidecar failure creates the inverse mismatch after reopen.
            // Never combine either pair of generations.
            if source.version() != version || source.source_generation() != source_generation {
                return None;
            }
            if !source.tags_known() {
                let recovered_tags = tag_ids
                    .iter()
                    .map(|&id| {
                        self.tag_dict
                            .key_value(id)
                            .map(|(key, value)| (key.to_owned(), value.to_owned()))
                    })
                    .collect::<Option<Vec<_>>>();
                source.recover_missing_tags(recovered_tags);
            }
            return Some(source);
        }
        // Footer-less v1/original-v2 sources carry neither trustworthy version nor raw
        // tags. They may pair only with a pre-v8 exact row: a non-zero exact
        // generation proves this is a stale sidecar and must fail loud.
        if source_generation != 0 || source.source_generation() != 0 {
            return None;
        }
        // Preserve the true legacy compatibility path by inheriting the newest
        // live exact row, reconstructing tags only when every id is reversible.
        let recovered_tags = tag_ids
            .iter()
            .map(|&id| {
                self.tag_dict
                    .key_value(id)
                    .map(|(key, value)| (key.to_owned(), value.to_owned()))
            })
            .collect::<Option<Vec<_>>>();
        source.recover_legacy_metadata(version, source_generation, recovered_tags);
        Some(source)
    }

    /// Whether the current snapshot has a live exact-store row for `logical_id`,
    /// independent of source-store availability.
    ///
    /// Point-read adapters use this after a failed source lookup to distinguish
    /// a legitimate missing document from a damaged/missing selected source sidecar.
    #[must_use]
    pub fn has_live_query(&self, logical_id: u64) -> bool {
        self.source_metadata_for_logical(logical_id).is_some()
    }

    /// Winner-fetch lookup with pre-allocation byte credit. `Err(actual_len)`
    /// means the current source exists but does not fit; the source store checks
    /// its borrowed resident/mmap value before cloning. Public so the v2
    /// handler's enrichment loop can enforce its byte budget BEFORE allocating
    /// the source `String` (the peak-memory bound, ADR-108/110).
    pub fn get_query_source_bounded(
        &self,
        logical_id: u64,
        max_bytes: usize,
    ) -> Result<Option<String>, usize> {
        let Some((_, source_generation, _)) = self.source_metadata_for_logical(logical_id) else {
            return Ok(None);
        };
        self.query_store
            .get_bounded_at_generation(logical_id, source_generation, max_bytes)
    }

    pub fn explain_hit(
        &self,
        logical_id: u64,
        title: &str,
    ) -> Option<crate::explain::ExplainDetail> {
        let source = self.get_query_source(logical_id)?;
        self.explain_source(logical_id, &source, title)
    }

    /// Compile a structured explanation from already-fetched current source.
    /// Ranked delivery uses this to fetch and budget each winner source once.
    pub fn explain_source(
        &self,
        logical_id: u64,
        source: &str,
        title: &str,
    ) -> Option<crate::explain::ExplainDetail> {
        let mut lc = String::new();
        let cq = crate::compile::compile_one_readonly(
            source,
            logical_id,
            &self.norm,
            &self.dict,
            &mut lc,
            self.config.hot_anchor_threshold,
        )
        .ok()?;
        Some(crate::explain::explain_match_structured(
            &cq, title, &self.norm, &self.dict,
        ))
    }

    pub fn class_counts(&self) -> [u64; 5] {
        let mut c = [0u64; 5];
        for seg in &self.segments {
            match seg.as_ref() {
                BaseSegment::Memory(s) => s.class_counts(&mut c),
                BaseSegment::Mmap(s) => s.class_counts(&mut c),
            }
        }
        self.memtable.class_counts(&mut c);
        // c[3] = STORED class-D always-candidates (ADR-068), symmetric with A/B/C;
        // rejections are the separate `rejected_class_d` metric.
        c
    }

    /// Per-segment introspection rows (base segments oldest-first, then the
    /// memtable), read lock-free from this snapshot. Backs the server's
    /// `GET /_cat/segments`. See [`SegmentInfo`](crate::events::SegmentInfo).
    pub fn segment_infos(&self) -> Vec<crate::events::SegmentInfo> {
        crate::segment::metrics::collect_segment_infos(
            &self.segments,
            &self.memtable,
            self.vocab_epoch,
        )
    }

    pub fn metrics(&self) -> crate::events::EngineMetrics {
        let segment_sizes: Vec<usize> = self.segments.iter().map(|s| s.len()).collect();
        let segment_holes: Vec<f64> = self.segments.iter().map(|s| s.holes_ratio()).collect();
        crate::events::EngineMetrics {
            total_queries: self.num_queries(),
            base_segments: self.segments.len(),
            memtable_entries: self.memtable.len(),
            segment_sizes,
            segment_holes,
            rejected_parse: self.rejected_parse,
            rejected_class_d: self.rejected_class_d,
            would_be_hot: self.would_be_hot,
            bodies_total: self.bodies_total,
            dup_joined: self.dup_joined,
            distinct_bodies_est: self.distinct_bodies_est,
            dict_features: self.dict.len(),
            exact_bytes: self.segments.iter().map(|s| s.exact_bytes()).sum::<usize>()
                + self.memtable.exact_bytes(),
            index_bytes: self
                .segments
                .iter()
                .map(|s| s.main_bytes() + s.broad_bytes() + s.hot_bytes())
                .sum::<usize>()
                + self.memtable.main_bytes()
                + self.memtable.broad_bytes()
                + self.memtable.hot_bytes(),
            filter_bytes: self
                .segments
                .iter()
                .map(|s| s.filter_bytes())
                .sum::<usize>(),
            stale_segments: self.stale_segment_count(),
            dict_bytes: self.dict.heap_bytes(),
            query_store_bytes: self.query_store.resident_bytes(),
            logical_index_bytes: self
                .segments
                .iter()
                .map(|s| s.logical_index_bytes())
                .sum::<usize>()
                + self.memtable.logical_index_bytes(),
            alive_bytes: self.segments.iter().map(|s| s.alive_bytes()).sum::<usize>()
                + self.memtable.alive_bytes(),
            wal_size_bytes: self.wal_size_bytes,
            wal_pending_entries: self.wal_pending_entries,
        }
    }

    /// Metadata for the newest live mutation of a logical id. The internal source
    /// generation is the ordering authority across storage tiers: an additive bulk
    /// write may create a newer base-segment row while an older live insert remains
    /// in the memtable. Reverse storage order is retained only as the tie-break for
    /// legacy generation-zero rows. Returns `None` if no live copy exists.
    pub(super) fn source_metadata_for_logical(
        &self,
        logical_id: u64,
    ) -> Option<(u32, u64, &[crate::tagdict::TagId])> {
        let mut best: Option<(u32, u64, &[crate::tagdict::TagId])> = None;
        for &local in self.memtable.locals_for_logical(logical_id).iter().rev() {
            if self.memtable.is_alive(local) {
                let source_generation = self.memtable.source_generation_of(local);
                let replace = match best {
                    Some((_, best_generation, _)) => source_generation > best_generation,
                    None => true,
                };
                if replace {
                    best = Some((
                        self.memtable.version_of(local),
                        source_generation,
                        self.memtable.tags_of(local),
                    ));
                }
            }
        }
        for seg in self.segments.iter().rev() {
            for &local in seg.locals_for_logical(logical_id).iter().rev() {
                if seg.is_alive(local) {
                    let source_generation = seg.source_generation_of(local);
                    let replace = match best {
                        Some((_, best_generation, _)) => source_generation > best_generation,
                        None => true,
                    };
                    if replace {
                        best = Some((seg.version_of(local), source_generation, seg.tags_of(local)));
                    }
                }
            }
        }
        best
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn point_read_refuses_source_from_a_different_exact_generation() {
        let mut engine =
            crate::segment::Engine::new(Normalizer::default_vocab().expect("normalizer"));
        engine
            .try_upsert_live_with_tags("1994 topps", 7, 1, &[("status".into(), "old".into())])
            .expect("initial upsert");
        let old_snapshot = engine.snapshot();

        // SourceStore is intentionally shared by snapshots. Mutate the engine without
        // publishing a replacement snapshot to reproduce the write/source ordering
        // window: old exact row, new source document.
        engine
            .try_upsert_live_with_tags("1995 fleer", 7, 1, &[("status".into(), "new".into())])
            .expect("replacement upsert");

        assert!(
            old_snapshot.get_query_document(7).is_none(),
            "the internal generation must reject a newer source even when both client versions are 1"
        );
        assert!(
            old_snapshot.has_live_query(7),
            "the mismatch is source unavailability, not document absence"
        );

        let fresh = engine
            .snapshot()
            .get_query_document(7)
            .expect("fresh generation is coherent");
        assert_eq!(fresh.version(), 1);
        assert_eq!(fresh.query(), "1995 fleer");
        assert_eq!(fresh.tags(), [("status".into(), "new".into())]);
    }

    #[test]
    fn point_read_finds_a_newer_bulk_generation_below_an_older_memtable_row() {
        let mut engine =
            crate::segment::Engine::new(Normalizer::default_vocab().expect("normalizer"));
        engine
            .try_insert_live("1994 topps", 7, 1)
            .expect("live insert");
        let report = engine.bulk_ingest(&[(7, "1995 fleer".to_string())]);
        assert_eq!(report.ingested, 1);

        let source = engine
            .snapshot()
            .get_query_document(7)
            .expect("bulk generation has a matching live exact row");
        assert_eq!(source.query(), "1995 fleer");
    }
}
