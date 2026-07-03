//! The durable cluster hot tier (class H, ADR-105): a θ-on corpus seals class-H
//! entries into per-shard **v5** segments, and the cluster reopens ≡ pre-crash ≡
//! brute (segment base + clog tail) across K ∈ {1, 3, 8}, on BOTH visibility
//! modes. A θ-flipped reopen is RESULT-identical (sealed entries keep their
//! recorded class; only new compiles reclassify — the ADR-105 benign-divergence
//! property). The `ClusterManifest` is deliberately NOT bumped for the hot tier
//! (cluster shards attach their registered segments fail-loud, so the v5 `.seg`
//! version word alone fences an old binary) — pinned here both ways: the version
//! stays 5, and a forged future segment version refuses the whole open.

use crate::harness::*;
const MANIFEST: &str = "cluster_manifest.bin";
/// θ for the durable corpus (~12k queries) — smaller than the 20k oracles' 64 so
/// the Zipf-head players still cross it; non-degeneracy is asserted.
const THETA: u32 = 32;

fn theta_cfg(num_shards: usize, dir: std::path::PathBuf, theta: u32) -> ClusterConfig {
    let mut cfg = durable_cfg(num_shards, dir, false);
    cfg.per_shard.hot_anchor_threshold = theta;
    cfg
}

/// Headline: a durable θ-on cluster (class H sealed in v5 segments + a clog-tail
/// live add) reopens ≡ pre-crash ≡ brute, broad on AND off, K ∈ {1, 3, 8}.
#[test]
fn durable_hot_cluster_reopens_and_matches() {
    let (queries, titles) = build_corpus();
    for &k in &[1usize, 3, 8] {
        let dir = unique_dir(&format!("hot_k{k}"));

        let (pre_broad, pre_sel, pre_h): (Vec<Vec<u64>>, Vec<Vec<u64>>, u64) = {
            let cluster =
                ClusterEngine::build(vocab(), &theta_cfg(k, dir.clone(), THETA), &queries)
                    .expect("durable cluster builds");
            let h = cluster.class_counts().expect("cc")[4];
            assert!(h > 0, "k={k}: degenerate — no class H stored");
            // A live add on the un-checkpointed clog tail (replayed on reopen).
            cluster
                .add_query(9_000_777, "2001 topps rareplayer3")
                .expect("live add");
            (
                titles
                    .iter()
                    .map(|t| cluster.percolate(t).expect("percolate"))
                    .collect(),
                titles
                    .iter()
                    .map(|t| cluster.percolate_with_broad(t, false).expect("percolate"))
                    .collect(),
                cluster.class_counts().expect("cc")[4],
            )
            // drop — no checkpoint: recovery replays the clog tail.
        };

        let cfg = theta_cfg(k, dir.clone(), THETA);
        let reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)).expect("reopen");
        assert_eq!(
            reopened.class_counts().expect("cc")[4],
            pre_h,
            "k={k}: class-H population drifted across reopen (same θ)"
        );
        let mut live = queries.clone();
        live.push((9_000_777, "2001 topps rareplayer3".to_string()));
        let brute = Brute::build(&live);
        let mut blc = String::new();
        let mut bfeats = Vec::new();
        for (i, title) in titles.iter().enumerate() {
            let broad = reopened.percolate(title).expect("percolate");
            assert_eq!(broad, pre_broad[i], "k={k}: broad-on drifted on {title:?}");
            let truth = brute.matches(title, &mut blc, &mut bfeats);
            assert_eq!(
                broad.iter().copied().collect::<HashSet<u64>>(),
                truth,
                "k={k}: reopened cluster vs brute on {title:?}"
            );
            let sel = reopened
                .percolate_with_broad(title, false)
                .expect("percolate");
            assert_eq!(
                sel, pre_sel[i],
                "k={k}: broad-OFF drifted on {title:?} — a sealed class-H entry \
                 went unreachable or invisible"
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}

/// The θ-flip reopen: results identical on both modes (sealed v5 entries keep
/// their recorded class and stay matchable); a NEW θ-hot add under the flipped
/// knob classifies A instead (the counts drift, the matches don't). Plus the
/// two fence pins: the ClusterManifest version stays 5 (deliberately NOT bumped
/// — the negative pin against an accidental v6), and a forged future `.seg`
/// version refuses the whole cluster open (the fail-loud attach that makes the
/// manifest bump unnecessary).
#[test]
fn reopen_with_flipped_theta_is_result_identical_and_fences_hold() {
    let (queries, titles) = build_corpus();
    let dir = unique_dir("hot_flip");
    let (pre_broad, pre_sel, pre_h): (Vec<Vec<u64>>, Vec<Vec<u64>>, u64) = {
        let cluster = ClusterEngine::build(vocab(), &theta_cfg(3, dir.clone(), THETA), &queries)
            .expect("durable cluster builds");
        let h = cluster.class_counts().expect("cc")[4];
        assert!(h > 0, "degenerate — no class H stored");
        cluster.checkpoint().expect("checkpoint");
        (
            titles
                .iter()
                .map(|t| cluster.percolate(t).expect("percolate"))
                .collect(),
            titles
                .iter()
                .map(|t| cluster.percolate_with_broad(t, false).expect("percolate"))
                .collect(),
            h,
        )
    };

    // The negative fence pin: hot-bearing data does NOT bump the cluster
    // manifest (the v5 ADR-080 marker is the current word; the per-shard v5
    // `.seg` files carry the hot fence).
    let mbytes = std::fs::read(dir.join(MANIFEST)).expect("manifest bytes");
    let version = u32::from_le_bytes(mbytes[4..8].try_into().expect("version word"));
    assert_eq!(
        version, 5,
        "the ClusterManifest must stay v5 — the hot fence lives in the segments"
    );

    // Reopen θ=0: identical results, sealed H intact; a new θ-hot-shaped add
    // classifies A under the flipped knob (counts drift, benign).
    let cfg0 = theta_cfg(3, dir.clone(), 0);
    let reopened = ClusterEngine::open(dir.clone(), vocab(), Some(&cfg0)).expect("reopen θ=0");
    assert_eq!(
        reopened.class_counts().expect("cc")[4],
        pre_h,
        "sealed class-H entries must keep their recorded class across a θ-flip"
    );
    for (i, title) in titles.iter().enumerate() {
        assert_eq!(
            reopened.percolate(title).expect("percolate"),
            pre_broad[i],
            "θ-flip reopen changed broad-on results on {title:?}"
        );
        assert_eq!(
            reopened.percolate_with_broad(title, false).expect("p"),
            pre_sel[i],
            "θ-flip reopen changed broad-off results on {title:?}"
        );
    }
    let before_add = reopened.class_counts().expect("cc");
    reopened
        .add_query(9_100_000, "1990 topps rareplayer7")
        .expect("live add under θ=0");
    let after_add = reopened.class_counts().expect("cc");
    assert_eq!(
        after_add[4], before_add[4],
        "a θ=0 reopen must classify new adds without class H"
    );
    drop(reopened);

    // The fail-loud attach fence: forge a REGISTERED shard segment to a future
    // version (re-sealing its CRC so the version check is what fires) — the
    // whole cluster open must refuse, not skip-and-continue.
    let m = read_cluster_manifest(&dir.join(MANIFEST)).expect("read manifest");
    let (shard_idx, seg_name) = m
        .segment_registry
        .iter()
        .enumerate()
        .find_map(|(si, files)| files.first().map(|f| (si, f.clone())))
        .expect("a registered segment");
    let seg_path = dir
        .join(format!("shard_{shard_idx:03}"))
        .join("segments")
        .join(&seg_name);
    let mut bytes = std::fs::read(&seg_path).expect("segment bytes");
    bytes[4..8].copy_from_slice(&9u32.to_le_bytes());
    let body = bytes.len() - 4;
    let crc = reverse_rusty::storage::crc32(&bytes[..body]);
    bytes[body..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&seg_path, &bytes).expect("write forged segment");
    let cfg = theta_cfg(3, dir.clone(), THETA);
    let msg = match ClusterEngine::open(dir.clone(), vocab(), Some(&cfg)) {
        Ok(_) => panic!("a forged future segment version must refuse the open"),
        Err(e) => e.to_string(),
    };
    assert!(
        msg.contains("unsupported format version") || msg.contains("format version"),
        "got: {msg}"
    );
    std::fs::remove_dir_all(&dir).ok();
}
