//! Vocabulary × durable reopen (ADR-076): the persisted-manifest vocab drives matching
//! from disk alone — `build_with_vocab` persists from the FIRST commit, a `set_vocab`
//! rebuild persists via its own checkpoint, and an EMPTY bare-manifest reopen activates
//! a file vocab through the same `set_vocab` funnel (the server's `--vocab` reopen path).

use crate::harness::*;
use crate::vocab_learning::vocab_with_multiword_alias;
use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};

fn stamp_cluster_segments_as_legacy(
    dir: &std::path::Path,
    manifest: &reverse_rusty::storage::ClusterManifest,
) {
    for (shard, files) in manifest.segment_registry.iter().enumerate() {
        for name in files {
            let path = dir
                .join(format!("shard_{shard:03}"))
                .join("segments")
                .join(name);
            let mut bytes = std::fs::read(&path).expect("read cluster segment");
            bytes[12..16].copy_from_slice(&0u32.to_le_bytes());
            let body = bytes.len() - 4;
            let crc = reverse_rusty::storage::crc32(&bytes[..body]);
            bytes[body..].copy_from_slice(&crc.to_le_bytes());
            std::fs::write(path, bytes).expect("write legacy compiler stamp");
        }
    }
}

/// Convert this binary's v7 cluster manifest to the exact v6 prefix: v6 ends
/// immediately after `placement_generation`; v7 appends compiler semantics +
/// the source-file column before the trailing CRC.
fn downgrade_cluster_manifest_to_v6(
    path: &std::path::Path,
    manifest: &reverse_rusty::storage::ClusterManifest,
) {
    let mut bytes = std::fs::read(path).expect("read v7 manifest");
    let v7_suffix = 4
        + 4
        + manifest
            .source_files
            .iter()
            .map(|name| 4 + name.len())
            .sum::<usize>();
    let v6_content_len = bytes
        .len()
        .checked_sub(4 + v7_suffix)
        .expect("v7 suffix fits");
    bytes.truncate(v6_content_len);
    bytes[4..8].copy_from_slice(&6u32.to_le_bytes());
    let crc = reverse_rusty::storage::crc32(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(path, bytes).expect("write v6 manifest");
}

fn visibility_token(mut n: usize) -> String {
    let mut suffix = [b'a'; 3];
    for byte in suffix.iter_mut().rev() {
        *byte += (n % 26) as u8;
        n /= 26;
    }
    format!(
        "maskword{}",
        std::str::from_utf8(&suffix).expect("ASCII suffix")
    )
}

#[test]
fn compiler_migration_preserves_the_frozen_mask_and_default_visibility() {
    // A compiler-only migration must not re-rank the persisted top-64 mask from
    // the post-delete live corpus. If it did, the rank-65 target below would
    // enter the mask after one hotter term is deleted and move from default-
    // visible class A to opt-in class C during reopen.
    let dir = std::env::temp_dir().join(format!("rr-adr118-frozen-mask-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = ClusterConfig {
        num_shards: 1,
        include_broad: true,
        data_dir: Some(dir.clone()),
        ..ClusterConfig::default()
    };
    let mut queries = Vec::new();
    for i in 0..64 {
        queries.push(((i * 2 + 1) as u64, visibility_token(i)));
        queries.push(((i * 2 + 2) as u64, visibility_token(i)));
    }
    let target_id = 1_000;
    let target = visibility_token(64);
    queries.push((target_id, target.clone()));

    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("durable build");
    assert!(
        cluster
            .percolate_with_broad(&target, false)
            .expect("default-visible read")
            .contains(&target_id),
        "the rank-65 target starts outside the mask and default-visible"
    );
    cluster.remove_query(1).expect("remove first hot row");
    cluster.remove_query(2).expect("remove second hot row");
    cluster.checkpoint().expect("checkpoint the deletes");
    assert!(
        cluster
            .percolate_with_broad(&target, false)
            .expect("post-delete read")
            .contains(&target_id),
        "deletes do not mutate the live frozen mask"
    );
    drop(cluster);

    let manifest = reverse_rusty::storage::read_cluster_manifest(&dir.join("cluster_manifest.bin"))
        .expect("cluster manifest");
    stamp_cluster_segments_as_legacy(&dir, &manifest);
    let reopened = ClusterEngine::open(
        &dir,
        reverse_rusty::normalize::Normalizer::default_vocab().expect("normalizer"),
        Some(&cfg),
    )
    .expect("compiler-migrating reopen");
    assert!(
        reopened
            .percolate_with_broad(&target, false)
            .expect("post-migration default-visible read")
            .contains(&target_id),
        "a compiler-only migration must not move an unrelated query behind include_broad"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

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

#[test]
fn durable_cluster_rebuilds_legacy_clause_boundary_semantics_before_serving() {
    let dir =
        std::env::temp_dir().join(format!("rr-adr118-clause-migration-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = ClusterConfig {
        num_shards: 3,
        replication_factor: 2,
        include_broad: true,
        data_dir: Some(dir.clone()),
        ..ClusterConfig::default()
    };
    {
        let cluster = ClusterEngine::build_with_vocab(
            vocab_with_multiword_alias(),
            &cfg,
            &[(1, "new -used york".into())],
        )
        .expect("durable build_with_vocab");
        assert!(
            cluster
                .percolate("new vintage collectible york")
                .unwrap()
                .contains(&1),
            "current compiler respects the negated-clause boundary"
        );
    }

    let before = reverse_rusty::storage::read_cluster_manifest(&dir.join("cluster_manifest.bin"))
        .expect("cluster manifest");
    stamp_cluster_segments_as_legacy(&dir, &before);

    // `open` may attach the legacy primary/replicas only inside this boot-time
    // transaction. It replays the coordinator log, rebuilds and re-places the
    // complete source corpus, bumps one placement generation, and atomically
    // commits the current-stamped green registry before returning.
    {
        let reopened = ClusterEngine::open(
            &dir,
            reverse_rusty::normalize::Normalizer::default_vocab().unwrap(),
            Some(&cfg),
        )
        .expect("migrating cluster reopen");
        assert_eq!(
            reopened.placement_generation().0,
            before.placement_generation.0 + 1
        );
        assert!(
            reopened
                .percolate("new vintage collectible york")
                .unwrap()
                .contains(&1),
            "cluster migration retains the clause-boundary match"
        );
        let current =
            reverse_rusty::storage::read_cluster_manifest(&dir.join("cluster_manifest.bin"))
                .expect("migrated manifest");
        assert!(current
            .segment_registry
            .iter()
            .enumerate()
            .flat_map(|(shard, files)| files.iter().map(move |name| (shard, name)))
            .all(|(shard, name)| {
                reverse_rusty::storage::MmapSegment::open(
                    &dir.join(format!("shard_{shard:03}"))
                        .join("segments")
                        .join(name),
                )
                .expect("open migrated cluster segment")
                .compiler_semantics_version()
                    > 0
            }));
    }

    let again = ClusterEngine::open(
        &dir,
        reverse_rusty::normalize::Normalizer::default_vocab().unwrap(),
        Some(&cfg),
    )
    .expect("second cluster reopen");
    assert_eq!(
        again.placement_generation().0,
        before.placement_generation.0 + 1,
        "current-stamped segments must not rebuild again"
    );
    assert!(again
        .percolate("new vintage collectible york")
        .unwrap()
        .contains(&1));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn durable_cluster_rebuilds_a_legacy_tail_even_when_the_base_is_empty() {
    let dir = std::env::temp_dir().join(format!("rr-adr118-tail-only-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        data_dir: Some(dir.clone()),
        ..ClusterConfig::default()
    };
    {
        let cluster = ClusterEngine::build_with_vocab(vocab_with_multiword_alias(), &cfg, &[])
            .expect("empty durable cluster");
        cluster
            .add_query(1, "new -used york")
            .expect("uncheckpointed tail add");
        // Drop without checkpoint: every committed segment registry is empty,
        // and the query exists only in the coordinator log tail.
    }
    let manifest_path = dir.join("cluster_manifest.bin");
    let before =
        reverse_rusty::storage::read_cluster_manifest(&manifest_path).expect("current manifest");
    assert!(before.segment_registry.iter().all(Vec::is_empty));
    downgrade_cluster_manifest_to_v6(&manifest_path, &before);

    let reopened = ClusterEngine::open(
        &dir,
        reverse_rusty::normalize::Normalizer::default_vocab().unwrap(),
        Some(&cfg),
    )
    .expect("tail-only compiler migration");
    assert!(
        reopened
            .percolate("new vintage collectible york")
            .unwrap()
            .contains(&1),
        "the raw legacy tail must be folded then placed under current semantics"
    );
    let current = reverse_rusty::storage::read_cluster_manifest(&manifest_path)
        .expect("migrated v7 manifest");
    assert_eq!(
        current.compiler_semantics_version, 3,
        "successful migration commits the current compiler marker"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn legacy_tail_preserves_acknowledged_tags_after_limit_tightening() {
    let dir = std::env::temp_dir().join(format!(
        "rr-adr118-tagged-tail-limit-{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    let wide = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        data_dir: Some(dir.clone()),
        per_shard: reverse_rusty::config::EngineConfig {
            max_tags: 4,
            ..Default::default()
        },
        ..ClusterConfig::default()
    };
    let tags = vec![
        ("tier".to_string(), "gold".to_string()),
        ("region".to_string(), "us".to_string()),
        ("channel".to_string(), "web".to_string()),
    ];
    {
        let cluster = ClusterEngine::build_with_vocab(vocab_with_multiword_alias(), &wide, &[])
            .expect("empty durable cluster");
        cluster
            .add_query_with_tags(1, "new -used york", &tags)
            .expect("tagged tail add accepted under the wider limit");
        // No checkpoint: the tagged query exists only in the coordinator tail.
    }

    let manifest_path = dir.join("cluster_manifest.bin");
    let before =
        reverse_rusty::storage::read_cluster_manifest(&manifest_path).expect("current manifest");
    downgrade_cluster_manifest_to_v6(&manifest_path, &before);

    let mut tight = wide.clone();
    tight.per_shard.max_tags = 2;
    let reopened = ClusterEngine::open(
        &dir,
        reverse_rusty::normalize::Normalizer::default_vocab().unwrap(),
        Some(&tight),
    )
    .expect("legacy tagged tail migrates despite the tighter current policy");
    let filter = vec![("tier".to_string(), vec!["gold".to_string()])];
    assert!(
        reopened
            .percolate_filtered("new vintage collectible york", &filter)
            .expect("filtered percolate")
            .contains(&1),
        "an acknowledged over-current-limit tag column must survive migration intact"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn failed_cluster_compiler_migration_preserves_old_sources_for_retry() {
    let dir = std::env::temp_dir().join(format!("rr-adr118-source-retry-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        data_dir: Some(dir.clone()),
        ..ClusterConfig::default()
    };
    {
        ClusterEngine::build_with_vocab(
            vocab_with_multiword_alias(),
            &cfg,
            &[(1, "new -used york".into())],
        )
        .expect("durable cluster");
    }
    let manifest_path = dir.join("cluster_manifest.bin");
    let before =
        reverse_rusty::storage::read_cluster_manifest(&manifest_path).expect("current manifest");
    downgrade_cluster_manifest_to_v6(&manifest_path, &before);
    let old_sources: Vec<Vec<u8>> = (0..cfg.num_shards)
        .map(|shard| {
            std::fs::read(dir.join(format!("shard_{shard:03}")).join("sources.dat"))
                .expect("old source sidecar")
        })
        .collect();

    // Poison the writer's atomic temp path. The migration may finish its green
    // segments/source sidecars, but cannot commit the v7 coordinator manifest.
    let manifest_tmp = dir.join("cluster_manifest.cmanifest.tmp");
    std::fs::create_dir(&manifest_tmp).expect("poison manifest temp");
    let first = ClusterEngine::open(
        &dir,
        reverse_rusty::normalize::Normalizer::default_vocab().unwrap(),
        Some(&cfg),
    );
    assert!(first.is_err(), "the injected manifest failure must surface");
    for (shard, expected) in old_sources.iter().enumerate() {
        assert_eq!(
            std::fs::read(dir.join(format!("shard_{shard:03}")).join("sources.dat"))
                .expect("old source survives"),
            *expected,
            "a pre-commit green rebuild must not overwrite shard {shard}'s old source generation"
        );
    }

    std::fs::remove_dir(&manifest_tmp).expect("clear injected failure");
    let reopened = ClusterEngine::open(
        &dir,
        reverse_rusty::normalize::Normalizer::default_vocab().unwrap(),
        Some(&cfg),
    )
    .expect("migration retry from the still-authoritative v6 manifest");
    assert!(reopened
        .percolate("new vintage collectible york")
        .unwrap()
        .contains(&1));
    let current =
        reverse_rusty::storage::read_cluster_manifest(&manifest_path).expect("retry committed v7");
    assert!(
        current
            .source_files
            .iter()
            .all(|name| name.starts_with("sources_g")),
        "the successful manifest must atomically select the green source generation"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
