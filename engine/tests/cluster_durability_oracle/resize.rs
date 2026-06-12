//! Resize durability (ADR-078, ADR-065 criterion 7): a resized cluster survives checkpoint +
//! reopen ≡ pre-crash ≡ the independent brute oracle; the manifest records the new shard count
//! with an INVARIANT dict fingerprint; a shrink removes the orphaned shard dirs (so a later
//! re-grow cannot self-restart a stale `new_durable` into an old corpus — the review's P2-1
//! changed-dir-set hazard); and the vocabulary + per-query tags carry through across the restart.

use crate::harness::*;

const MANIFEST: &str = "cluster_manifest.bin";

/// Count top-level `shard_NNN` directories under a cluster `data_dir`.
fn count_shard_dirs(dir: &std::path::Path) -> usize {
    std::fs::read_dir(dir)
        .expect("read cluster dir")
        .flatten()
        .filter(|e| {
            e.file_name()
                .to_str()
                .is_some_and(|n| n.starts_with("shard_"))
        })
        .count()
}

/// The declared alias `zzabbr ≡ zzcanon` as a `Vocab` (mirrors the vocab durability suite).
fn alias_vocab() -> reverse_rusty::vocab::Vocab {
    let mut v = reverse_rusty::vocab::Vocab::new();
    v.add_synonym(
        "zzabbr",
        "term:zzcanon",
        reverse_rusty::dict::FeatureKind::Generic,
    );
    v
}

/// Assert a reopened cluster ≡ the captured pre-crash snapshot ≡ the brute oracle.
fn assert_reopened(
    reopened: &ClusterEngine,
    titles: &[String],
    pre: &[HashSet<u64>],
    brute: &Brute,
    ctx: &str,
) {
    let mut lc = String::new();
    let mut feats: Vec<u32> = Vec::new();
    for (i, t) in titles.iter().enumerate() {
        let got: HashSet<u64> = reopened.percolate(t).unwrap().into_iter().collect();
        assert_eq!(got, pre[i], "{ctx}: reopened ≠ pre-crash on {t:?}");
        let want = brute.matches(t, &mut lc, &mut feats);
        assert_eq!(got, want, "{ctx}: reopened ≠ brute oracle on {t:?}");
    }
}

#[test]
fn resize_grow_survives_checkpoint_and_reopen() {
    let (queries, titles) = build_corpus();
    let brute = Brute::build(&queries);
    let check: Vec<String> = titles.iter().take(150).cloned().collect();

    for &(k0, k1) in &[(1usize, 4usize), (3, 8)] {
        let dir = unique_dir(&format!("resize_grow_{k0}to{k1}"));
        let pre: Vec<HashSet<u64>> = {
            let mut cluster =
                ClusterEngine::build(vocab(), &durable_cfg(k0, dir.clone(), false), &queries)
                    .expect("durable build");
            let rebuilt = cluster.resize(k1).expect("resize (internally checkpoints)");
            assert!(rebuilt > 0);
            assert_eq!(cluster.num_shards(), k1);
            check
                .iter()
                .map(|t| cluster.percolate(t).unwrap().into_iter().collect())
                .collect()
        };
        // The manifest is the atomic commit point: it now records the new shard count.
        let m = read_cluster_manifest(&dir.join(MANIFEST)).expect("manifest");
        assert_eq!(m.num_shards as usize, k1, "{k0}->{k1}: manifest num_shards");

        let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
        assert_eq!(reopened.num_shards(), k1, "{k0}->{k1}: reopened num_shards");
        assert_reopened(&reopened, &check, &pre, &brute, &format!("grow {k0}->{k1}"));
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn resize_shrink_survives_reopen_and_removes_orphan_dirs() {
    let (queries, titles) = build_corpus();
    let brute = Brute::build(&queries);
    let check: Vec<String> = titles.iter().take(150).cloned().collect();

    for &(k0, k1) in &[(8usize, 3usize), (3, 1)] {
        let dir = unique_dir(&format!("resize_shrink_{k0}to{k1}"));
        let pre: Vec<HashSet<u64>> = {
            let mut cluster =
                ClusterEngine::build(vocab(), &durable_cfg(k0, dir.clone(), false), &queries)
                    .expect("durable build");
            cluster.resize(k1).expect("resize shrink");
            assert_eq!(cluster.num_shards(), k1);
            check
                .iter()
                .map(|t| cluster.percolate(t).unwrap().into_iter().collect())
                .collect()
        };
        // A shrink leaves EXACTLY `shard_000..shard_{k1-1}` on disk (orphans removed).
        assert_eq!(
            count_shard_dirs(&dir),
            k1,
            "{k0}->{k1}: expected exactly {k1} shard dirs after shrink"
        );
        let m = read_cluster_manifest(&dir.join(MANIFEST)).expect("manifest");
        assert_eq!(m.num_shards as usize, k1, "{k0}->{k1}: manifest num_shards");

        let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
        assert_reopened(
            &reopened,
            &check,
            &pre,
            &brute,
            &format!("shrink {k0}->{k1}"),
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn shrink_then_regrow_does_not_resurrect_deleted_queries() {
    // The ADR-078 changed-dir-set hazard (review P2-1), made DISCRIMINATING: orphan shard dirs
    // from a shrink must be removed (`remove_orphan_shard_dirs`) and any re-grown position must
    // build CLEAN (`clean_shard_dir`), so a query DELETED while the cluster was small cannot be
    // resurrected by a stale dir when the cluster grows back. Without the dir cleanup, a re-grown
    // position self-restarts `new_durable` from the leftover sidecar and resurrects the deleted
    // query — the oracle + the explicit `!contains` checks catch it.
    let (queries, titles) = build_corpus();
    // Distinctive deletable queries spread across anchors, so several land on positions ≥ 3 —
    // i.e. on the dirs a shrink-to-3 orphans (the resurrection vector).
    let mut full = queries.clone();
    let mut delenda: Vec<(u64, String)> = Vec::new();
    for i in 0..24u64 {
        let id = 9_990_000 + i;
        let dsl = format!("zzdelendum{i} zzanchor{i}");
        full.push((id, dsl.clone()));
        delenda.push((id, dsl));
    }
    // The FINAL live set is the originals only (every delendum is deleted), so the independent
    // oracle is built WITHOUT the delenda.
    let brute = Brute::build(&queries);
    let extra_title = |d: &str| format!("{d} extra");
    let check: Vec<String> = titles
        .iter()
        .take(120)
        .cloned()
        .chain(delenda.iter().map(|(_, d)| extra_title(d)))
        .collect();

    let dir = unique_dir("shrink_regrow_delete");
    let pre: Vec<HashSet<u64>> = {
        let mut cluster = ClusterEngine::build(vocab(), &durable_cfg(8, dir.clone(), false), &full)
            .expect("durable build k8");
        for (id, d) in &delenda {
            assert!(
                cluster.percolate(&extra_title(d)).unwrap().contains(id),
                "delendum {id} should match before deletion"
            );
        }
        cluster.resize(3).expect("shrink to 3"); // orphans + removes shard_003..007
        for (id, _) in &delenda {
            cluster.remove_query(*id).expect("delete delendum");
        }
        cluster.resize(8).expect("re-grow to 8"); // shard_003..007 rebuilt CLEAN
        assert_eq!(cluster.num_shards(), 8);
        for (id, d) in &delenda {
            assert!(
                !cluster.percolate(&extra_title(d)).unwrap().contains(id),
                "delendum {id} resurrected by a stale shard dir after re-grow"
            );
        }
        check
            .iter()
            .map(|t| cluster.percolate(t).unwrap().into_iter().collect())
            .collect()
    };
    assert_eq!(
        count_shard_dirs(&dir),
        8,
        "re-grow restores exactly 8 shard dirs"
    );
    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen after re-grow");
    assert_reopened(&reopened, &check, &pre, &brute, "shrink->delete->regrow");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn resize_preserves_dict_fingerprint() {
    // The normalizer is unchanged across a resize, so the re-minted dict is identical ⇒ the
    // manifest's dict fingerprint must be invariant (a spurious change would desync the
    // control-plane fingerprint and the recovery handshakes).
    let (queries, _titles) = build_corpus();
    let dir = unique_dir("resize_fp");
    let mut cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
        .expect("durable build");
    let fp_before = read_cluster_manifest(&dir.join(MANIFEST))
        .expect("manifest after build")
        .dict_fingerprint;

    cluster.resize(8).expect("resize");
    let m_after = read_cluster_manifest(&dir.join(MANIFEST)).expect("manifest after resize");
    assert_eq!(m_after.num_shards, 8);
    assert_eq!(
        m_after.dict_fingerprint, fp_before,
        "dict fingerprint must be invariant across a resize"
    );
    drop(cluster);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn resize_preserves_dict_fingerprint_for_unsorted_corpus_with_post_freeze_add() {
    // The DISCRIMINATING fingerprint test (codex P2): re-minting the dict over the sorted live
    // corpus renumbers feature ids when the ORIGINAL build interned in a different order, or when
    // a post-freeze term was added (interned dense by a re-mint, but only a synthetic id in the
    // live dict) — a spurious fingerprint change that desyncs the control-plane / manifest.
    // Reusing the frozen dict makes the fingerprint invariant BY CONSTRUCTION. This corpus is
    // supplied OUT of logical-id order AND gets a post-freeze add, so a re-mint WOULD change the
    // fingerprint (the pre-fix bug) — the reuse path keeps it stable.
    let queries = vec![
        (90u64, "zulu uniquealpha".to_string()),
        (10u64, "yankee uniquebravo".to_string()),
        (50u64, "xray uniquecharlie".to_string()),
    ];
    let dir = unique_dir("resize_fp_unsorted");
    let mut cluster = ClusterEngine::build(vocab(), &durable_cfg(2, dir.clone(), false), &queries)
        .expect("durable build (ids out of order)");
    cluster
        .add_query(70, "zzbrandnewterm uniquedelta")
        .expect("post-freeze add (a brand-new token ⇒ synthetic id)");
    let fp_before = read_cluster_manifest(&dir.join(MANIFEST))
        .expect("manifest after build")
        .dict_fingerprint;

    cluster.resize(4).expect("resize");
    let m_after = read_cluster_manifest(&dir.join(MANIFEST)).expect("manifest after resize");
    assert_eq!(m_after.num_shards, 4);
    assert_eq!(
        m_after.dict_fingerprint, fp_before,
        "the dict fingerprint must be invariant across a resize even for an out-of-order corpus + \
         a post-freeze add (a re-mint would renumber ids and change it)"
    );
    drop(cluster);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn same_count_resize_recommits_on_a_durable_cluster() {
    // P1 (codex): a same-K resize must not bare-acknowledge — a prior resize could have swapped
    // the ring in RAM and then failed to checkpoint, leaving the manifest at the old count, so a
    // retry (which hits this same-K path) must re-ensure the durable commit rather than mask it.
    // Observable: a same-count resize on a DURABLE cluster re-checkpoints (the epoch bumps),
    // which is exactly what heals an un-committed prior resize; it still rebuilds nothing.
    let (queries, _titles) = build_corpus();
    let dir = unique_dir("resize_same_k");
    let mut cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
        .expect("durable build");
    let epoch_before = cluster.epoch();
    let rebuilt = cluster.resize(3).expect("same-count resize");
    assert_eq!(rebuilt, 0, "a same-count resize rebuilds nothing");
    assert_eq!(cluster.num_shards(), 3);
    assert!(
        cluster.epoch() > epoch_before,
        "a same-count resize on a durable cluster re-checkpoints (heals an un-committed prior \
         resize) rather than bare-acking"
    );
    drop(cluster);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn resize_then_live_add_survives_reopen() {
    // A query added AFTER a resize lands under the NEW ring and is logged (not yet
    // checkpointed); on reopen the log tail replays over the resized manifest. Zero FN.
    let (queries, _titles) = build_corpus();
    let dir = unique_dir("resize_then_add");
    let newq = 9_800_001u64;
    let newtitle = "1994 fleer zznewplayer psa 10";
    {
        let mut cluster =
            ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
                .expect("durable build");
        cluster.resize(6).expect("resize"); // checkpoints at K=6
        cluster
            .add_query(newq, "1994 fleer zznewplayer")
            .expect("add after resize"); // logged only
    }
    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    assert_eq!(reopened.num_shards(), 6);
    assert!(
        reopened.percolate(newtitle).unwrap().contains(&newq),
        "a query added after the resize must survive reopen (log replay over the resized manifest)"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn tagged_resize_carries_tags_across_reopen() {
    // A tagged durable cluster resized + reopened: filtered percolation ≡ the brute oracle
    // filtered by the same tags. Tags ride the stored `TagId`s through the ring change and the
    // mmap-backed segments on reopen.
    let (queries, titles) = build_corpus();
    let tags = tags_parallel(&queries);
    let brute = Brute::build(&queries);
    let dir = unique_dir("resize_tags");
    {
        let mut cluster = ClusterEngine::build_with_tags(
            vocab(),
            &durable_cfg(3, dir.clone(), false),
            &queries,
            &tags,
        )
        .expect("tagged durable build");
        cluster.resize(8).expect("resize");
    }
    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    let mut blc = String::new();
    let mut bfeats = Vec::new();
    for (ti, title) in titles.iter().take(120).enumerate() {
        let truth = brute.matches(title, &mut blc, &mut bfeats);
        for filter in filters_for(ti) {
            let got: HashSet<u64> = reopened
                .percolate_filtered(title, &filter)
                .unwrap()
                .into_iter()
                .collect();
            let want: HashSet<u64> = truth
                .iter()
                .copied()
                .filter(|l| passes_filter(&tags_for(*l), &filter))
                .collect();
            assert_eq!(
                got, want,
                "tagged resize+reopen: filtered ≠ oracle (title {ti}, filter {filter:?})"
            );
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn vocab_survives_resize_and_reopen() {
    // A declared alias installed via set_vocab must survive a subsequent resize (which reuses the
    // normalizer and re-resolves the EXISTING vocab's equivalences) AND the reopen after it — the
    // ADR-078 vocab-preservation property (resize passes `None`, so `self.vocab` is kept).
    let (mut queries, _titles) = build_corpus();
    let qid = 9_900_001u64;
    queries.push((qid, "1994 fleer zzabbr".into()));
    let title = "1994 fleer zzcanon psa 10";
    let dir = unique_dir("resize_vocab");
    {
        let mut cluster =
            ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), false), &queries)
                .expect("durable build");
        cluster.set_vocab(alias_vocab()).expect("set_vocab");
        assert!(
            cluster.percolate(title).unwrap().contains(&qid),
            "the alias is active before the resize"
        );
        cluster.resize(8).expect("resize");
        assert!(
            cluster.percolate(title).unwrap().contains(&qid),
            "the alias must survive the resize"
        );
    }
    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    assert!(
        reopened.percolate(title).unwrap().contains(&qid),
        "the alias must survive resize + reopen"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
