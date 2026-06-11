//! Codex retro-review of ADR-074 — the stale-source-store family. The blue/green
//! rebuild's green engines used to inherit the dir's old `sources.dat`, so a query
//! whose re-placement left a shard stayed behind there as a stale store entry; the
//! next rebuild's store-driven gather then preferred the stale (untagged / deleted)
//! copy whenever the abandoned shard preceded the live copies in iteration order.
//! Fixed at the GATHER: it now cross-checks index liveness, so stale residue is
//! never gathered (and dirs polluted pre-fix self-heal). The green stores keep
//! LOADING the old `sources.dat` deliberately — the green ingest persists sources
//! EAGERLY, before the manifest commit, so the on-disk store must stay a SUPERSET
//! of every generation a crash-reopen could make authoritative (codex round 2: a
//! bucket-only green store lost moved-away queries if the crash landed before the
//! commit). Plus the build-commit guard: a durable build whose `sources.dat` write
//! failed must refuse to commit its manifest rather than ack an incomplete corpus.
//!
//! Placement mechanics these constructions rely on (probed empirically while
//! building this suite): a declared EQUIVALENCE (ADR-054) widens the query's anchor
//! into an any-of, which places it on the REPLICATED lane — shard 0, always first in
//! gather order — while UNBINDING it sends the query back to its selective anchor
//! shard. A synonym, by contrast, merely renames the anchor token: the re-minted
//! dict interns it at the same sequence position, same `FeatureId`, same ring slot —
//! no movement at all. So bind→unbind is the deterministic mover: the abandoned
//! shard is shard 0, which ALWAYS precedes the live selective shard.

use crate::harness::*;

/// The declared equivalence group `{a, b}` as a Vocab (the ADR-054 expansion path).
fn equiv_vocab(a: &str, b: &str) -> reverse_rusty::vocab::Vocab {
    let mut v = reverse_rusty::vocab::Vocab::new();
    v.add_equivalence(&[a, b]);
    v
}

#[test]
fn unbind_then_rebuild_does_not_lose_the_moved_querys_tags() {
    // The deterministic tag-loss shape: BIND (the widened query moves to the
    // replicated lane, shard 0, tags carried) → UNBIND via an empty vocab (the query
    // returns to its selective anchor shard; pre-fix, shard 0's green store kept its
    // entry) → one more rebuild. Shard 0 is gathered FIRST, so the pre-fix gather
    // locked in the stale UNTAGGED copy via first-wins dedup — the rebuilt query
    // matched, but its filter tags were silently gone (the codex finding).
    let (mut queries, _titles) = build_corpus();
    let q = 9_500_001u64;
    queries.push((q, "1994 fleer zzmovea".into()));
    let tags = tags_parallel(&queries);
    let q_tag = tags_for(q); // interned at build via build_with_tags
    let filter = vec![(q_tag[0].0.clone(), vec![q_tag[0].1.clone()])];
    let title = "1994 fleer zzmovea psa 10";

    let dir = unique_dir("tagloss_unbind");
    let mut cluster = ClusterEngine::build_with_tags(
        vocab(),
        &durable_cfg(8, dir.clone(), false),
        &queries,
        &tags,
    )
    .expect("tagged durable build");
    cluster
        .set_vocab(equiv_vocab("zzmovea", "zzcanona"))
        .expect("bind: the widened query moves to the replicated lane");
    // Preconditions pinned: the expansion is ACTIVE (the canon surface form reaches
    // the query) and the tags survived the first rebuild.
    assert!(
        cluster
            .percolate("1994 fleer zzcanona psa 10")
            .unwrap()
            .contains(&q),
        "precondition — the equivalence must widen the query onto zzcanona"
    );
    assert!(
        cluster
            .percolate_filtered(title, &filter)
            .unwrap()
            .contains(&q),
        "precondition — tags carried through the bind rebuild"
    );
    cluster
        .set_vocab(reverse_rusty::vocab::Vocab::new())
        .expect("unbind: the query returns to its selective shard");
    assert!(
        !cluster
            .percolate("1994 fleer zzcanona psa 10")
            .unwrap()
            .contains(&q),
        "precondition — the unbind is active (the canon form no longer reaches it)"
    );
    // The next rebuild's gather sees whatever the green stores persisted. Pre-fix:
    // shard 0's stale untagged entry, FIRST.
    cluster
        .set_vocab(reverse_rusty::vocab::Vocab::new())
        .expect("rebuild over the post-unbind stores");
    assert!(
        cluster.percolate(title).unwrap().contains(&q),
        "the query itself survives every rebuild (only its TAGS were at risk)"
    );
    assert!(
        cluster
            .percolate_filtered(title, &filter)
            .unwrap()
            .contains(&q),
        "the moved query's interned tags must survive the unbind + rebuild \
         (the stale shard-0 store entry used to shadow the tagged copy)"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rebuild_does_not_resurrect_a_moved_then_deleted_query() {
    // The resurrection shape: BIND moves the query off its build shard (onto the
    // replicated lane), abandoning a store entry there pre-fix. DELETE then scrubs a
    // store only where it tombstones a live copy — the abandoned shard keeps its
    // entry. The next rebuild's gather found the deleted query's stale entry — the
    // ONLY store entry for that id — so first-wins dedup re-placed it
    // unconditionally, order-independent: a deleted query matching again.
    let (mut queries, _titles) = build_corpus();
    let q = 9_500_002u64;
    queries.push((q, "1994 fleer zzmovea".into()));
    let dir = unique_dir("resurrect_moved");
    let mut cluster = ClusterEngine::build(vocab(), &durable_cfg(8, dir.clone(), false), &queries)
        .expect("durable build");
    cluster
        .set_vocab(equiv_vocab("zzmovea", "zzcanona"))
        .expect("bind: the widened query moves off its build shard");
    assert!(
        cluster
            .percolate("1994 fleer zzcanona psa 10")
            .unwrap()
            .contains(&q),
        "precondition — the equivalence must widen the query onto zzcanona"
    );
    assert!(cluster.remove_query(q).expect("delete") >= 1);
    assert!(
        !cluster
            .percolate("1994 fleer zzmovea psa 10")
            .unwrap()
            .contains(&q),
        "deleted before the rebuild"
    );
    let rebuilt = cluster
        .set_vocab(equiv_vocab("zzmovea", "zzcanona"))
        .expect("rebuild after the delete");
    assert_eq!(
        rebuilt,
        queries.len() - 1,
        "the rebuild covers exactly the live corpus — a stale store entry on the \
         abandoned build shard must not re-place the deleted query"
    );
    for title in ["1994 fleer zzmovea psa 10", "1994 fleer zzcanona psa 10"] {
        assert!(
            !cluster.percolate(title).unwrap().contains(&q),
            "{title:?}: the vocabulary rebuild must NOT resurrect a moved-then-deleted \
             query from a stale source-store entry"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn durable_build_refuses_when_a_sources_write_fails() {
    // The build-commit guard (the sources-half twin of `segment_filenames`' in-memory
    // fallback check): poison every shard's `sources.dat` path by pre-creating it as
    // a DIRECTORY, so the bulk seal's source-store write fails. The build must refuse
    // to commit its manifest — acking would leave durable segments whose reopen
    // gathers an empty corpus, and a later `set_vocab` would silently erase the
    // queries it can't see.
    let (queries, _titles) = build_corpus();
    let dir = unique_dir("sources_write_fails");
    for s in 0..3 {
        std::fs::create_dir_all(dir.join(format!("shard_{s:03}")).join("sources.dat"))
            .expect("poison the sources path");
    }
    let Err(err) = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
    else {
        panic!("a durable build whose sources.dat write failed must refuse to commit")
    };
    let msg = err.to_string();
    assert!(
        msg.contains("durability write failed") || msg.contains("sources"),
        "the refusal names the failed durability write (got: {msg})"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn green_sources_remain_a_superset_across_the_rebuild() {
    // The crash-window invariant (codex round 2 on this branch): the green ingest
    // persists `sources.dat` EAGERLY — before the rebuild's manifest commit — so the
    // file must remain a SUPERSET of every generation a crash-reopen could make
    // authoritative. A bucket-only green store would lose moved-away queries when a
    // crash lands before the commit: the old manifest + old segments stay
    // authoritative, but the abandoned shard's store no longer lists the query, and
    // the next rebuild's gather (store ∩ live) silently drops it. Keeping stale
    // residue is safe (the liveness-checked gather skips it); dropping live history
    // is not. Pinned at the file level: after a committed rebuild that moves a query
    // onto the replicated lane, its source must still be present in MORE THAN ONE
    // shard store — the new home AND the abandoned original — not just the new home.
    let (mut queries, _titles) = build_corpus();
    let q = 9_500_003u64;
    queries.push((q, "1994 fleer zzmovea".into()));
    let dir = unique_dir("superset_sources");
    let mut cluster = ClusterEngine::build(vocab(), &durable_cfg(8, dir.clone(), false), &queries)
        .expect("durable build");
    cluster
        .set_vocab(equiv_vocab("zzmovea", "zzcanona"))
        .expect("bind: the widened query moves onto the replicated lane");
    assert!(
        cluster
            .percolate("1994 fleer zzcanona psa 10")
            .unwrap()
            .contains(&q),
        "precondition — the equivalence must widen the query onto zzcanona"
    );
    drop(cluster); // committed by set_vocab's own checkpoint
    let mut holders = 0usize;
    for s in 0..8 {
        let path = dir.join(format!("shard_{s:03}")).join("sources.dat");
        if path.exists() {
            let store = reverse_rusty::storage::SourceStore::open(&path, true)
                .expect("open a shard's committed source store");
            if store.get(q).is_some() {
                holders += 1;
            }
        }
    }
    assert!(
        holders >= 2,
        "the moved query's source must survive on its abandoned shard too (the \
         crash-window superset), not only on its new home — found it in {holders} store(s)"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
