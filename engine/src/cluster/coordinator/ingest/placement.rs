use super::{placement_of, ClusterEngine, Extracted, ParseError, ParseErrorKind, Target};

impl ClusterEngine {
    /// The placement decision for one compiled query — see the module-level table.
    /// Delegates to the free [`placement_of`] so `build` can bucket the corpus before
    /// the cluster value exists.
    pub(super) fn placement(&self, ex: &Extracted) -> Target {
        placement_of(
            &self.dict,
            &self.ring,
            ex,
            self.per_shard.accept_class_d,
            self.per_shard.hot_anchor_threshold,
        )
    }

    /// True if the cluster holds (or has ever held) any tagged query (ADR-055): the `tags_present`
    /// latch (any tagged write, incl. post-freeze *synthetic* tags never interned into `tag_dict`)
    /// OR a non-empty `tag_dict` (build-time interned tags). Operator introspection only
    /// ([`Self::has_tagged_queries`]) — the vocab rebuild carries tags by stored `TagId` (ADR-074)
    /// and no longer consults this.
    pub(in crate::cluster::coordinator) fn has_tags(&self) -> bool {
        self.tags_present.load(std::sync::atomic::Ordering::Relaxed) || !self.tag_dict.is_empty()
    }

    /// Reject a tag set larger than the per-shard `max_tags` ceiling (ADR-049) at the
    /// cluster front door, BEFORE the mutation reaches the log — so an over-large set
    /// never truncates the shards' u16 tag column (which would silently drop a real tag
    /// and mis-filter). Mirrors the single-node `Engine::check_tag_limit`; conservative
    /// (raw `(key,value)` count, `>=` the post-dedup column count). Replay does not call
    /// this (an acknowledged write must never be dropped on recovery).
    pub(in crate::cluster::coordinator) fn check_tag_limit(
        &self,
        tags: &[(String, String)],
    ) -> Result<(), ParseError> {
        if tags.len() > self.per_shard.max_tags {
            return Err(ParseError::new(ParseErrorKind::TooManyTags, 0));
        }
        Ok(())
    }

    /// Reject a COMPILED query whose required / forbidden / any-of column would overflow
    /// the shards' SoA exact-store `u16` count encoding, BEFORE the mutation reaches the
    /// log — so the truncating cast in `ExactStore::push` is never reached on apply (a
    /// truncated store is a silent false negative). Cluster analogue of the single-node
    /// `Engine::check_column_limit`; runs on the read-only-compiled `Extracted` (after
    /// equivalence expansion). See [`Extracted::column_overflow`](crate::compile::Extracted::column_overflow).
    pub(in crate::cluster::coordinator) fn check_column_limit(
        ex: &crate::compile::Extracted,
    ) -> Result<(), ParseError> {
        if ex.column_overflow().is_some() {
            return Err(ParseError::new(ParseErrorKind::CompiledColumnTooLarge, 0));
        }
        Ok(())
    }

    /// Latch [`tags_present`](ClusterEngine::tags_present) when a non-empty tagged write happens.
    /// Cheap + idempotent; no-op for an untagged write (the byte-identical path).
    pub(in crate::cluster::coordinator) fn note_tags(&self, tags: &[(String, String)]) {
        if !tags.is_empty() {
            self.tags_present
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }
}
