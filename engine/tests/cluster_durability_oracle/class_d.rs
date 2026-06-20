//! The durable cluster class-D always-candidate lane (ADR-080): a class-D corpus stored
//! under `accept_class_d` is sealed into per-shard segments + a v5 `ClusterManifest` (the
//! rollback fence), and reopens byte-equal to pre-crash AND equal to the independent brute
//! that keeps class-D — across K ∈ {1, 3, 8}, including a clog-tail live add. Reopening with
//! the knob OFF keeps the sealed always-candidates matchable (the knob gates acceptance, not
//! visibility) while rejecting a new class-D add — the cluster analogue of the single-node
//! reopen-with-knob-flip pin.

use crate::harness::*;
use reverse_rusty::cluster::AddOutcome;
use reverse_rusty::gen::gen_class_d_queries;

/// Logical-id offset for the negation-only queries (every generated regular id is far below).
const CLASS_D_ID_BASE: u64 = 1_000_000;
const MANIFEST: &str = "cluster_manifest.bin";

/// A durable cluster config with the always-candidate lane on.
fn lane_on_cfg(num_shards: usize, dir: std::path::PathBuf) -> ClusterConfig {
    let mut cfg = durable_cfg(num_shards, dir, false);
    cfg.per_shard.accept_class_d = true;
    cfg
}

/// `build_corpus` (class A + B + C) with negation-only queries appended (class D). A
/// class-D query `-<term>` matches every title NOT bearing `<term>`, so a corpus of
/// out-of-vocab forbidden terms matches broadly — a strong durable differential.
fn corpus_with_class_d(n_class_d: usize) -> (Vec<(u64, String)>, Vec<String>) {
    let (mut queries, titles) = build_corpus();
    for (di, dsl) in gen_class_d_queries(0xD00D_C1D5, n_class_d)
        .into_iter()
        .enumerate()
    {
        queries.push((CLASS_D_ID_BASE + di as u64, dsl));
    }
    (queries, titles)
}

/// Headline: a durable class-D cluster reopens ≡ pre-crash ≡ brute (segment base + clog
/// tail), the build commit writes the v5 fence, across K ∈ {1, 3, 8}.
#[test]
fn durable_cluster_class_d_reopens_and_matches() {
    let (queries, titles) = corpus_with_class_d(200);
    for &k in &[1usize, 3, 8] {
        let dir = unique_dir(&format!("class_d_k{k}"));

        // Build durable (lane on): class-D sealed into the segment base + a v5 manifest. Add
        // one more class-D live so the un-checkpointed clog tail also carries an
        // always-candidate (recovery replays it under the lane).
        let pre_crash: Vec<Vec<u64>> = {
            let cluster = ClusterEngine::build(vocab(), &lane_on_cfg(k, dir.clone()), &queries)
                .expect("durable cluster builds");
            cluster
                .add_query(CLASS_D_ID_BASE + 900, "-tailonly")
                .expect("live class-D add");
            titles
                .iter()
                .map(|t| cluster.percolate(t).expect("percolate"))
                .collect()
            // drop — no checkpoint: recovery replays the clog tail (the live add).
        };

        // The build-time commit wrote the v5 rollback fence (class-D present).
        let m = read_cluster_manifest(&dir.join(MANIFEST)).expect("manifest");
        assert!(
            m.class_d_fence,
            "k={k}: class-D commit must write the v5 fence"
        );

        // Reopen (lane on, consistent with build): class-D survives the segment base AND the
        // clog-tail replay.
        let cfg = lane_on_cfg(k, dir.clone());
        let reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).expect("reopen");
        assert!(
            reopened.class_counts().expect("cc")[3] > 0,
            "k={k}: no class-D after reopen"
        );

        let mut live = queries.clone();
        live.push((CLASS_D_ID_BASE + 900, "-tailonly".to_string()));
        let brute = Brute::build_accepting_class_d(&live);
        let mut lc = String::new();
        let mut feats: Vec<u32> = Vec::new();
        for (i, t) in titles.iter().enumerate() {
            let got: HashSet<u64> = reopened
                .percolate(t)
                .expect("percolate")
                .into_iter()
                .collect();
            assert_eq!(
                got,
                brute.matches(t, &mut lc, &mut feats),
                "k={k} reopened≠brute on {t:?}"
            );
            let pre: HashSet<u64> = pre_crash[i].iter().copied().collect();
            assert_eq!(got, pre, "k={k} reopened≠pre-crash on {t:?}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// The knob gates acceptance, not visibility: a sealed always-candidate stays matchable when
/// the cluster is reopened with `accept_class_d` OFF, while a NEW negation-only add is rejected.
#[test]
fn sealed_class_d_survives_reopen_with_knob_off_but_new_adds_rejected() {
    let (queries, titles) = corpus_with_class_d(100);
    let dir = unique_dir("class_d_knob_off");

    // Build durable (lane on) — class-D sealed into the segment base, no clog tail.
    {
        let _cluster = ClusterEngine::build(vocab(), &lane_on_cfg(4, dir.clone()), &queries)
            .expect("durable cluster builds");
    }

    // Reopen with the knob OFF: the sealed always-candidates still match (segments are
    // attached regardless of the knob), but a new negation-only add is rejected.
    let cfg_off = durable_cfg(4, dir.clone(), false); // accept_class_d defaults to false
    let reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg_off)).expect("reopen");
    assert!(
        reopened.class_counts().expect("cc")[3] > 0,
        "sealed class-D lost on a knob-off reopen — visibility must not depend on the knob"
    );

    let brute = Brute::build_accepting_class_d(&queries);
    let mut lc = String::new();
    let mut feats: Vec<u32> = Vec::new();
    for t in &titles {
        let got: HashSet<u64> = reopened
            .percolate(t)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(
            got,
            brute.matches(t, &mut lc, &mut feats),
            "sealed class-D not visible after knob-off reopen on {t:?}"
        );
    }

    assert_eq!(
        reopened
            .add_query(CLASS_D_ID_BASE + 5000, "-freshreject")
            .expect("add"),
        AddOutcome::RejectedClassD,
        "the knob still gates the front door after a knob-off reopen"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
