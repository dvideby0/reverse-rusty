//! Vocabulary × durable reopen (ADR-076): the persisted-manifest vocab drives matching
//! from disk alone — `build_with_vocab` persists from the FIRST commit, a `set_vocab`
//! rebuild persists via its own checkpoint, and an EMPTY bare-manifest reopen activates
//! a file vocab through the same `set_vocab` funnel (the server's `--vocab` reopen path).

use crate::harness::*;
use crate::vocab_learning::vocab_with_multiword_alias;
use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};

#[test]
fn build_with_vocab_persists_the_vocab_from_the_first_durable_commit() {
    // Review finding: the durable `build_with_vocab` path (vocab_data written by
    // `commit_durable_base` at BUILD time — no set_vocab, no explicit checkpoint)
    // had no test. The ADR's crash-window claim: a crash before any later checkpoint
    // still reopens with the vocabulary in effect. Reopen with a BARE default
    // normalizer; the manifest's persisted vocab must drive matching from disk alone.
    let dir = std::env::temp_dir().join(format!("rr-adr076-bwv-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        data_dir: Some(dir.clone()),
        ..ClusterConfig::default()
    };
    {
        let cluster = ClusterEngine::build_with_vocab(
            vocab_with_multiword_alias(),
            &cfg,
            &[(1, "ny".into())],
        )
        .expect("durable build_with_vocab");
        for title in ["ny psa 10", "new york psa 10"] {
            assert!(
                cluster.percolate(title).unwrap().contains(&1),
                "pre-reopen: {title:?} must match"
            );
        }
        // Dropped WITHOUT a checkpoint: the build's own commit is the only durable state.
    }
    let reopened = ClusterEngine::open(
        &dir,
        reverse_rusty::normalize::Normalizer::default_vocab().unwrap(),
        None,
    )
    .expect("reopen from the build-time manifest");
    for title in ["ny psa 10", "new york psa 10"] {
        assert!(
            reopened.percolate(title).unwrap().contains(&1),
            "post-reopen: {title:?} must still match (vocab persisted at the FIRST commit)"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn multiword_alias_survives_durable_checkpoint_and_reopen() {
    // ADR-076 (flips the ADR-061 durable refusal): a multi-word alias activated via
    // `set_vocab` on a DURABLE cluster persists through the manifest's vocab blob —
    // after checkpoint + reopen the alias (and its P(T)-aware routing) is still in
    // effect: both surface forms match the alias-anchored query from disk alone.
    let dir = std::env::temp_dir().join(format!("rr-adr076-durable-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        data_dir: Some(dir.clone()),
        ..ClusterConfig::default()
    };
    {
        let mut cluster =
            ClusterEngine::build(vocab(), &cfg, &[(1, "ny".into())]).expect("durable build");
        cluster
            .set_vocab(vocab_with_multiword_alias())
            .expect("set_vocab activates the multi-word alias on a durable cluster");
        for title in ["ny psa 10", "new york psa 10"] {
            assert!(
                cluster.percolate(title).unwrap().contains(&1),
                "pre-reopen: {title:?} must match"
            );
        }
        // set_vocab already checkpointed (the durable rebuild commits itself).
    }
    let reopened = ClusterEngine::open(
        &dir,
        reverse_rusty::normalize::Normalizer::default_vocab().unwrap(),
        None,
    )
    .expect("reopen restores the persisted multi-word vocab from the manifest");
    for title in ["ny psa 10", "new york psa 10"] {
        assert!(
            reopened.percolate(title).unwrap().contains(&1),
            "post-reopen: {title:?} must still match (the persisted vocab drives \
             P(T)-aware routing from disk alone)"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn vocab_file_activates_on_an_empty_durable_reopen() {
    // Codex review (ADR-076): reopening an EMPTY durable cluster whose manifest never
    // persisted a vocabulary (a bare pre-vocab build), then supplying a vocab file +
    // load file, used to ingest with the equivalence machinery silently inert — and the
    // vocab stayed unpersisted, so the NEXT reopen lost it entirely. The server's reopen
    // path now activates the file vocab via `set_vocab` before ingesting; this pins the
    // engine-level seam it relies on: open(bare manifest) → set_vocab → ingest ≡ a fresh
    // `build_with_vocab`, including persistence (the rebuild's own durable checkpoint).
    let dir = std::env::temp_dir().join(format!("rr-adr076-reopen-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        data_dir: Some(dir.clone()),
        ..ClusterConfig::default()
    };
    {
        // A bare durable build: NO vocabulary, NO queries — just a committed manifest.
        let cluster = ClusterEngine::build(vocab(), &cfg, &[]).expect("bare durable build");
        cluster.checkpoint().expect("commit the bare manifest");
    }
    {
        let file_vocab = vocab_with_multiword_alias();
        let norm = file_vocab.to_normalizer().expect("file vocab → normalizer");
        let mut cluster =
            ClusterEngine::open(&dir, norm, Some(&cfg)).expect("reopen the bare manifest");
        // Precondition pinned: a bare manifest restores no vocabulary (if a future change
        // persists one here, this test stops exercising the activation path — fail loud).
        assert!(
            cluster.vocab().is_none(),
            "precondition: a bare manifest must restore no vocabulary"
        );
        assert_eq!(
            cluster.num_queries().unwrap(),
            0,
            "precondition: empty corpus"
        );
        cluster
            .set_vocab(file_vocab)
            .expect("activate the file vocab on the empty reopened cluster");
        cluster
            .ingest(&[(1, "ny".into())])
            .expect("ingest under the activated vocabulary");
        for title in ["ny psa 10", "new york psa 10"] {
            assert!(
                cluster.percolate(title).unwrap().contains(&1),
                "post-activation: {title:?} must match (equivalence machinery installed \
                 before ingest)"
            );
        }
    }
    // The next reopen — with a BARE normalizer — must still carry the vocabulary: the
    // activation persisted it in the manifest (the pre-fix path lost it here).
    let reopened = ClusterEngine::open(
        &dir,
        reverse_rusty::normalize::Normalizer::default_vocab().unwrap(),
        None,
    )
    .expect("reopen restores the activated vocab from the manifest");
    for title in ["ny psa 10", "new york psa 10"] {
        assert!(
            reopened.percolate(title).unwrap().contains(&1),
            "post-reopen: {title:?} must still match (the activation persisted the vocab)"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
