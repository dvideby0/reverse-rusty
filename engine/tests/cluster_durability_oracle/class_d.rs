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

        // The build-time commit wrote the v5 ADR-080 replicate-to-all marker.
        let m = read_cluster_manifest(&dir.join(MANIFEST)).expect("manifest");
        assert!(
            m.broad_replicate_all,
            "k={k}: an ADR-080 cluster must write the v5 replicate-to-all marker"
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

/// ADR-080 FORWARD fence: a pre-ADR-080 durable cluster placed the broad lane on shard 0 only;
/// this binary's rotating broad-eval shard would silently miss it. Such a cluster (manifest
/// v<5, no replicate-to-all marker) must be REFUSED on open, not mis-routed. Modeled by
/// downgrading a fresh v5 manifest's version word to v4 and re-sealing its CRC — the data is
/// irrelevant; the version marker is the gate.
#[test]
fn open_refuses_a_pre_adr080_cluster_loudly() {
    let (queries, _titles) = corpus_with_class_d(20);
    let dir = unique_dir("pre_adr080");
    {
        let cluster = ClusterEngine::build(vocab(), &lane_on_cfg(3, dir.clone()), &queries)
            .expect("durable cluster builds");
        cluster.checkpoint().expect("checkpoint");
    }

    // Downgrade the manifest version word 5 -> 4 (a legacy / pre-ADR-080 cluster) + re-seal the
    // trailing whole-file CRC, so the layout fence — not the CRC — is what fires on open.
    let mpath = dir.join(MANIFEST);
    let mut bytes = std::fs::read(&mpath).expect("read manifest");
    assert_eq!(
        u32::from_le_bytes(bytes[4..8].try_into().unwrap()),
        5,
        "a fresh ADR-080 cluster commits manifest v5"
    );
    bytes[4..8].copy_from_slice(&4u32.to_le_bytes());
    let body = bytes.len() - 4;
    let crc = reverse_rusty::storage::crc32(&bytes[..body]);
    bytes[body..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&mpath, &bytes).expect("rewrite manifest");

    // open must refuse loudly rather than silently mis-route the shard-0-only broad lane.
    let result = ClusterEngine::open(dir.clone(), vocab(), None);
    assert!(
        result.is_err(),
        "open must refuse a pre-ADR-080 cluster, but it succeeded"
    );
    if let Err(ShardError::Config(msg)) = result {
        assert!(
            msg.contains("predates ADR-080") || msg.contains("rebuild"),
            "expected a 'predates ADR-080 / rebuild' refusal, got: {msg}"
        );
    } else {
        panic!("expected a ShardError::Config refusal for a pre-ADR-080 cluster");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A rebuild (resize / set_vocab) must preserve already-stored class-D entries even when the
/// cluster was reopened with `accept_class_d` OFF — an always-candidate was accepted when added,
/// so the front-door knob must not drop it on a rebuild (the cluster analogue of ADR-068's
/// "vocab recompile passes accept=true unconditionally"). The bug: `rebuild_from_live` re-placed
/// the stored queries through the current (off) knob, dropping class D, and the checkpoint then
/// committed that deletion permanently.
#[test]
fn resize_with_knob_off_preserves_sealed_class_d() {
    let (queries, titles) = corpus_with_class_d(120);
    let dir = unique_dir("class_d_resize_knob_off");
    {
        let cluster = ClusterEngine::build(vocab(), &lane_on_cfg(3, dir.clone()), &queries)
            .expect("durable cluster builds");
        cluster.checkpoint().expect("checkpoint");
    }

    // Reopen with the knob OFF, then resize (a rebuild that re-places every stored query).
    let cfg_off = durable_cfg(3, dir.clone(), false);
    let mut reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg_off)).expect("reopen");
    assert!(
        reopened.class_counts().expect("cc")[3] > 0,
        "sealed class-D present after the knob-off reopen"
    );
    reopened.resize(5).expect("resize 3 -> 5");
    assert!(
        reopened.class_counts().expect("cc")[3] > 0,
        "class-D dropped by a knob-off resize — acknowledged always-candidates lost"
    );

    // And still matches exactly the brute that keeps class-D.
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
            "post-resize class-D differential on {t:?}"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
