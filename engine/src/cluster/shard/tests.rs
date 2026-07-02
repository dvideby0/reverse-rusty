//! Unit tests for the translog retention-lease bookkeeping (ADR-040/048).

#[cfg(test)]
mod retention_lease_tests {
    use crate::cluster::shard::retention::RetentionLeases;
    use std::time::{Duration, Instant};

    // ADR-048: the TTL reap drops a lease that has not heartbeated within the window (a
    // crashed/stalled recovery) while keeping one that renewed recently (a live recovery).
    #[test]
    fn reap_expired_drops_stale_keeps_renewed() {
        // Synthetic instants are built by ADDING to `t0` (never subtracting) and the offsets are
        // not whole-minute multiples, so the clock math is panic-free and unit-clean.
        let ttl = Duration::from_secs(100);
        let t0 = Instant::now();
        let mut leases = RetentionLeases::default();

        // Two recoveries each pin a tail position; the floor is the min.
        let stale = leases.acquire(10, t0);
        let live = leases.acquire(20, t0);
        assert_eq!(
            leases.floor(),
            Some(10),
            "floor is the lowest pinned position"
        );

        // The live recovery heartbeats (renew) well inside the window (80s < ttl); the stale one
        // never does (last heartbeat stays t0).
        leases.renew(live, 25, t0 + Duration::from_secs(80));

        // Reap as of t0+150s: the stale lease (idle 150s > ttl) is expired; the live lease (idle
        // 150-80 = 70s < ttl) survives.
        let now = t0 + Duration::from_secs(150);
        let reaped = leases.reap_expired(now, ttl);
        assert_eq!(reaped, 1, "only the un-renewed lease is reaped");
        assert_eq!(
            leases.floor(),
            Some(25),
            "the renewed lease survives and still pins its (advanced) tail"
        );

        // Releasing the survivor clears the floor entirely; the reaped one is already gone.
        leases.release(live);
        assert_eq!(leases.floor(), None);
        let _ = stale;
    }

    // A reap with nothing past the TTL is a no-op (disabled-equivalent behavior within the window).
    #[test]
    fn reap_expired_keeps_everything_within_the_window() {
        let ttl = Duration::from_secs(100);
        let t0 = Instant::now();
        let mut leases = RetentionLeases::default();
        leases.acquire(5, t0);
        let reaped = leases.reap_expired(t0 + Duration::from_secs(50), ttl);
        assert_eq!(reaped, 0);
        assert_eq!(leases.floor(), Some(5));
    }
}

/// ADR-097: the order-independent live-set content fingerprint — insertion order, flush
/// boundaries, and segment layout must NOT change it; version/tag/live-set changes MUST.
#[cfg(feature = "distributed")]
mod content_fingerprint_tests {
    use std::sync::Arc;

    use crate::cluster::shard::LocalShard;
    use crate::config::EngineConfig;
    use crate::dict::Dict;
    use crate::normalize::Normalizer;
    use crate::tagdict::TagDict;

    /// (norm, frozen dict, finalized tag space, per-query `(id, Extracted, dsl)`) — what
    /// [`compile`] returns (the coordinator's pass-A shape; mirrors replica/test_support).
    type Compiled = (
        Arc<Normalizer>,
        Arc<Dict>,
        Arc<TagDict>,
        Vec<(u64, crate::compile::Extracted, String)>,
    );

    /// Compile `(id, DSL)` into a shared frozen dict + finalized tag space + per-query
    /// `Extracted`.
    fn compile(dsls: &[(u64, &str)]) -> Compiled {
        let norm = Arc::new(Normalizer::default_vocab().expect("built-in vocab"));
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut out = Vec::new();
        for (id, dsl) in dsls {
            let ast = crate::dsl::parse(dsl).expect("test dsl parses");
            let ex = crate::compile::extract(&ast, &norm, &mut dict, &mut lc);
            out.push((*id, ex, (*dsl).to_string()));
        }
        dict.finalize_mask();
        let mut tag_dict = TagDict::new();
        tag_dict.mark_finalized();
        (norm, Arc::new(dict), Arc::new(tag_dict), out)
    }

    fn shard_over(norm: &Arc<Normalizer>, dict: &Arc<Dict>, tags: &Arc<TagDict>) -> LocalShard {
        LocalShard::new(
            Arc::clone(norm),
            Arc::clone(dict),
            Arc::clone(tags),
            EngineConfig::default(),
        )
    }

    const CORPUS: &[(u64, &str)] = &[(1, "+nike +shoe"), (2, "+sony +tv"), (3, "+lego +set")];

    /// Equal live sets fingerprint equal regardless of INSERTION ORDER and FLUSH BOUNDARIES —
    /// the property byte-level segment CRCs structurally cannot provide (and the reason the
    /// skip is sound across copies with divergent segment layouts). Also proves the memtable
    /// is included: shard B never flushes.
    #[test]
    fn order_and_layout_independent_and_memtable_included() {
        use crate::cluster::shard::Shard;
        let (norm, dict, tags, compiled) = compile(CORPUS);
        let a = shard_over(&norm, &dict, &tags);
        for (id, ex, dsl) in &compiled {
            a.insert_extracted_with_tags(ex, *id, 1, dsl, &[])
                .expect("insert a");
        }
        a.flush().expect("flush a to segments");
        let b = shard_over(&norm, &dict, &tags);
        for (id, ex, dsl) in compiled.iter().rev() {
            b.insert_extracted_with_tags(ex, *id, 1, dsl, &[])
                .expect("insert b (reverse order)");
        }
        // b stays memtable-only: layout-divergent by construction.
        let fa = a.content_fingerprint128();
        let fb = b.content_fingerprint128();
        assert_eq!(fa, fb, "equal live sets fingerprint equal across layouts");
        assert_eq!(fa.2, CORPUS.len() as u64, "the live count rides along");
    }

    /// A stored VERSION change (the ADR-067 upsert basis) changes the fingerprint — a member
    /// holding a stale version of a query is never \"provably complete\".
    #[test]
    fn version_sensitive() {
        use crate::cluster::shard::Shard;
        let (norm, dict, tags, compiled) = compile(CORPUS);
        let a = shard_over(&norm, &dict, &tags);
        let b = shard_over(&norm, &dict, &tags);
        for (id, ex, dsl) in &compiled {
            a.insert_extracted_with_tags(ex, *id, 1, dsl, &[])
                .expect("insert a");
            let vb = if *id == 2 { 7 } else { 1 };
            b.insert_extracted_with_tags(ex, *id, vb, dsl, &[])
                .expect("insert b");
        }
        assert_ne!(
            a.content_fingerprint128(),
            b.content_fingerprint128(),
            "a divergent stored version must change the fingerprint"
        );
    }

    /// A TAG difference changes the fingerprint — tags gate filtered percolation (ADR-049), so
    /// a copy with divergent tags is match-relevantly different.
    #[test]
    fn tag_sensitive() {
        use crate::cluster::shard::Shard;
        let (norm, dict, tags, compiled) = compile(CORPUS);
        let a = shard_over(&norm, &dict, &tags);
        let b = shard_over(&norm, &dict, &tags);
        for (id, ex, dsl) in &compiled {
            a.insert_extracted_with_tags(ex, *id, 1, dsl, &[])
                .expect("insert a");
            let tagged = [("team".to_string(), "alpha".to_string())];
            let bt: &[(String, String)] = if *id == 1 { &tagged } else { &[] };
            b.insert_extracted_with_tags(ex, *id, 1, dsl, bt)
                .expect("insert b");
        }
        assert_ne!(
            a.content_fingerprint128(),
            b.content_fingerprint128(),
            "a divergent tag set must change the fingerprint"
        );
    }

    /// A DELETE changes the fingerprint back to the smaller set's — the live MULTISET is the
    /// basis, not the write history.
    #[test]
    fn delete_sensitive_and_history_free() {
        use crate::cluster::shard::Shard;
        let (norm, dict, tags, compiled) = compile(CORPUS);
        let a = shard_over(&norm, &dict, &tags);
        for (id, ex, dsl) in &compiled[..2] {
            a.insert_extracted_with_tags(ex, *id, 1, dsl, &[])
                .expect("insert a (two queries)");
        }
        let b = shard_over(&norm, &dict, &tags);
        for (id, ex, dsl) in &compiled {
            b.insert_extracted_with_tags(ex, *id, 1, dsl, &[])
                .expect("insert b (all three)");
        }
        assert_ne!(
            a.content_fingerprint128(),
            b.content_fingerprint128(),
            "different live sets differ"
        );
        b.delete_by_logical_id(compiled[2].0).expect("delete third");
        assert_eq!(
            a.content_fingerprint128(),
            b.content_fingerprint128(),
            "after the delete the LIVE sets are equal — history leaves no residue"
        );
    }
}
