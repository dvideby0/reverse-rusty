//! `impl Engine` — the observer hook ([`set_observer`](Engine::set_observer) /
//! [`clear_observer`](Engine::clear_observer) / [`emit`](Engine::emit)), runtime
//! config get/set, [`snapshot`](Engine::snapshot), and the read-only engine-handle
//! accessors (dict / normalizer / segment filenames / query source / explain).

use crate::segment::{BaseSegment, Engine, EngineSnapshot};
use std::sync::Arc;

use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::normalize::Normalizer;
use crate::wal::Wal;

impl Engine {
    /// Set an observer callback that receives [`EngineEvent`](crate::events::EngineEvent)s
    /// for flush, compaction, ingest, and other lifecycle events. The callback
    /// must be `Send + Sync` (safe to call from rayon threads). Pass `None` to
    /// clear a previously set observer.
    ///
    /// Any events buffered during construction/recovery (e.g. a
    /// [`DurabilityFailure`](crate::events::EngineEvent::DurabilityFailure) from a
    /// corrupt segment skipped in [`open`](Self::open)) are delivered to the
    /// observer synchronously here, before this returns, then cleared — so an
    /// operator who wires the observer right after `open` still sees the recovery
    /// diagnostics through the structured stack.
    pub fn set_observer<F: Fn(&crate::events::EngineEvent) + Send + Sync + 'static>(
        &mut self,
        observer: F,
    ) {
        self.observer = Some(Box::new(observer));
        if !self.pending_events.is_empty() {
            let pending = std::mem::take(&mut self.pending_events);
            if let Some(ref cb) = self.observer {
                for ev in &pending {
                    cb(ev);
                }
            }
        }
    }

    /// Clear the observer callback.
    pub fn clear_observer(&mut self) {
        self.observer = None;
    }

    /// Emit an event to the observer (if set). No-op when no observer is registered.
    // The event is built at the call site solely to be emitted, then dropped; taking
    // it by value (vs `&`) costs nothing and keeps every call site free of `&` noise.
    #[allow(clippy::needless_pass_by_value)]
    #[inline]
    pub(in crate::segment) fn emit(&self, event: crate::events::EngineEvent) {
        if let Some(ref cb) = self.observer {
            cb(&event);
        }
    }

    /// Read-only access to the current configuration.
    pub fn config(&self) -> &EngineConfig {
        &self.config
    }

    /// Replace the runtime tuning configuration (copy-on-write: swaps in a new
    /// `Arc`, so any already-published snapshot keeps its own view).
    ///
    /// Only the **dynamic** knobs take effect retroactively — compaction/flush
    /// thresholds, query-complexity limits, merge cost, and the auto-compact
    /// flags are re-read on the next maintenance decision. The **static** fields
    /// (`data_dir`, `wal_sync_on_write`, `retain_source`) are bound at
    /// construction — the data dirs, WAL fsync policy, and source-store mode are
    /// already established — so they must equal the current values; changing them
    /// here has no retroactive effect and may split on-disk state. The server's
    /// `PUT /_settings` enforces this by rejecting those keys as non-dynamic.
    pub fn set_config(&mut self, config: EngineConfig) {
        self.config = Arc::new(config);
    }

    /// Create an immutable [`EngineSnapshot`] of the current read-path state.
    ///
    /// This is O(number of base segments) pointer copies, *not* O(corpus): the
    /// normalizer, dictionary, each base segment, the memtable, and the query
    /// store are all shared structurally via `Arc` (segments by per-segment
    /// pointer; the dict/memtable copy-on-write on the next write). Publishing a
    /// snapshot after every mutation is therefore cheap — the deep-clone-the-whole-
    /// engine cost the audit flagged (P1-16) is gone. Readers match against the
    /// snapshot without holding any lock on the engine.
    pub fn snapshot(&self) -> EngineSnapshot {
        EngineSnapshot {
            norm: Arc::clone(&self.norm),
            dict: Arc::clone(&self.dict),
            tag_dict: Arc::clone(&self.tag_dict),
            segments: self.segments.clone(),
            memtable: Arc::clone(&self.memtable),
            query_store: Arc::clone(&self.query_store),
            vocab: self.vocab.clone(),
            config: Arc::clone(&self.config),
            rejected_parse: self.rejected_parse,
            rejected_class_d: self.rejected_class_d,
            would_be_hot: self.would_be_hot,
            bodies_total: self.bodies_total,
            dup_joined: self.dup_joined,
            distinct_bodies_est: self.distinct_bodies_estimate(),
            vocab_epoch: self.vocab_epoch,
            wal_healthy: self.wal_healthy,
            persistence_healthy: self.persistence_healthy,
            skipped_segments: self.skipped_segments,
            wal_size_bytes: self.wal.as_ref().map_or(0, Wal::size_bytes),
            wal_pending_entries: self.wal.as_ref().map_or(0, Wal::pending_entries),
        }
    }

    /// Read-only access to the shared feature dictionary.
    pub fn dict(&self) -> &Dict {
        &self.dict
    }
    /// Read-only access to the normalizer.
    pub fn normalizer(&self) -> &Normalizer {
        &self.norm
    }

    /// The current next segment-id counter — recorded per shard in the cluster manifest
    /// so a flush after reopen never reuses a committed segment filename (ADR-032).
    pub fn next_seg_id(&self) -> u64 {
        self.next_seg_id
    }

    /// Whether every best-effort durability write so far succeeded — `false` after e.g.
    /// a failed `sources.dat` write (surfaced as a `DurabilityFailure` event, ADR-021).
    /// The cluster's durable BUILD checks this before committing its manifest
    /// ([`segment_filenames`](Self::segment_filenames) is the segments-half of the same
    /// guard): acking a build whose source store failed to persist would hand a later
    /// vocabulary rebuild an incomplete gather corpus (codex retro-review, ADR-074).
    pub fn persistence_healthy(&self) -> bool {
        self.persistence_healthy
    }

    /// The filenames of this engine's live (mmap'd) base segments, in order — the
    /// per-shard registry the cluster coordinator commits (ADR-032). Returns `Err` if
    /// ANY base segment is in-memory: that means a segment write fell back to `Memory`
    /// (e.g. a disk error, `persistence.rs`), and committing a registry that omits it
    /// would silently lose that segment's data on reopen, so the caller must refuse to
    /// commit and surface the failure instead.
    pub fn segment_filenames(&self) -> std::io::Result<Vec<String>> {
        let mut names = Vec::with_capacity(self.segments.len());
        for seg in &self.segments {
            match seg.as_ref() {
                BaseSegment::Mmap(m) => {
                    let name = m
                        .path()
                        .file_name()
                        .and_then(|f| f.to_str())
                        .ok_or_else(|| {
                            std::io::Error::new(
                                std::io::ErrorKind::InvalidData,
                                "segment path has no filename",
                            )
                        })?;
                    names.push(name.to_string());
                }
                BaseSegment::Memory(_) => {
                    return Err(std::io::Error::other(
                        "a base segment is in-memory (segment write fell back); refusing \
                         to commit a cluster segment registry that would lose it on reopen",
                    ));
                }
            }
        }
        Ok(names)
    }

    /// Look up the original query text for a logical ID. Returns `None` if
    /// the ID was never ingested or has been deleted.
    pub fn get_query_source(&self, logical_id: u64) -> Option<String> {
        self.query_store.get(logical_id)
    }

    /// Explain why a stored query matched (or would match) a given title.
    /// Re-derives the CompiledQuery from stored source text using the
    /// read-only compile path. Returns `None` if the query source is
    /// unavailable.
    pub fn explain_hit(
        &self,
        logical_id: u64,
        title: &str,
    ) -> Option<crate::explain::ExplainDetail> {
        let source = self.get_query_source(logical_id)?;
        let mut lc = String::new();
        let cq = crate::compile::compile_one_readonly(
            &source,
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
}
