//! ADR-113 cluster PIT + cursor oracle: pit-scoped paging over the in-process
//! cluster concatenates to exactly the one-shot distributed result AND the
//! single-node standalone reference, stays pinned across live mutation, and
//! every invalidation shape (resize / set_vocab / reopen / TTL / no-pit
//! boundary) fails closed as typed.

use std::time::{Duration, Instant};

use crate::harness::*;
use reverse_rusty::cluster::{ClusterConfig, ClusterEngine, ClusterPitError, ClusterRankedError};
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::{PitConfig, PitError, QueryScope, RankProgramSpec, TopKOptions};

fn rank_program() -> RankProgramSpec {
    RankProgramSpec {
        priority_field: Some("priority".to_string()),
        boosts: vec![("category".to_string(), "cards".to_string(), 1_000)],
    }
}

fn ranked_tags_parallel(queries: &[(u64, String)]) -> Vec<Vec<(String, String)>> {
    queries
        .iter()
        .map(|(l, _)| {
            let mut tags = tags_for(*l);
            if l % 5 == 0 {
                tags.push(("priority".to_string(), (l % 97).to_string()));
            }
            tags
        })
        .collect()
}

fn options(size: usize, scope: QueryScope, after: Option<(i64, u64)>) -> TopKOptions {
    TopKOptions {
        size,
        track_total_hits_up_to: 10_000,
        query_scope: scope,
        search_after: after,
    }
}

/// Page a pit to exhaustion, asserting per-page totals equal the one-shot's.
fn page_all(
    cluster: &ClusterEngine,
    pit: reverse_rusty::PitId,
    title: &str,
    scope: QueryScope,
    size: usize,
    program: &reverse_rusty::CompiledRankProgram,
    expected_total: reverse_rusty::TotalHits,
) -> Vec<(u64, i64)> {
    let mut pages = Vec::new();
    let mut after = None;
    loop {
        let page = cluster
            .try_percolate_filtered_top_k_pit(
                pit,
                title,
                &[],
                options(size, scope, after),
                program,
                None,
                Instant::now(),
            )
            .expect("pit page");
        assert_eq!(
            page.total_hits, expected_total,
            "pinned totals are page-invariant"
        );
        if page.hits.is_empty() {
            break;
        }
        after = page.hits.last().map(|hit| (hit.score, hit.logical_id));
        let full = page.hits.len() == size;
        pages.extend(page.hits.iter().map(|hit| (hit.logical_id, hit.score)));
        if !full {
            break;
        }
    }
    pages
}

/// The cluster exit gate: for K-shard clusters (RF 1 and 2), pit pages
/// concatenate to the one-shot pit result ≡ the standalone single-node
/// reference, while live adds/removes/upserts between pages change nothing
/// under the PIT — and a fresh current-view read sees the mutated world.
#[test]
fn pit_pages_concatenate_pin_across_mutation_and_match_single_node() {
    let (queries, titles) = build_corpus();
    let tags = ranked_tags_parallel(&queries);
    let program = rank_program();

    let mut reference = Engine::new(vocab());
    reference
        .try_build_from_queries_with_tags(&queries, &tags)
        .expect("tagged reference build");
    let reference = reference.snapshot();
    let reference_program = reference
        .compile_rank_program(&program)
        .expect("reference rank program");
    let predicate = reverse_rusty::exact::TagPredicate::empty();
    let mut scratch = MatchScratch::new();

    for &(shards, rf) in &[(1usize, 1usize), (3, 1), (8, 1), (3, 2)] {
        let cfg = ClusterConfig {
            num_shards: shards,
            replication_factor: rf,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags)
            .expect("tagged cluster build");
        let cluster_program = cluster
            .compile_rank_program(&program)
            .expect("cluster rank program");
        let pit = cluster
            .open_pit(None, &PitConfig::default(), Instant::now())
            .expect("open pit");

        let mut fresh_id = 5_000_000u64;
        for (i, title) in titles.iter().take(12).enumerate() {
            for scope in [QueryScope::Standard, QueryScope::WithBroad] {
                let want = reference
                    .try_match_title_top_k(
                        title,
                        options(1_000, scope, None),
                        &reference_program,
                        &predicate,
                        &mut scratch,
                        None,
                    )
                    .expect("standalone one-shot");
                let want_rows: Vec<(u64, i64)> = want
                    .hits
                    .iter()
                    .map(|hit| (hit.logical_id, hit.score))
                    .collect();

                let one_shot = cluster
                    .try_percolate_filtered_top_k_pit(
                        pit,
                        title,
                        &[],
                        options(1_000, scope, None),
                        &cluster_program,
                        None,
                        Instant::now(),
                    )
                    .expect("pit one-shot");
                let one_shot_rows: Vec<(u64, i64)> = one_shot
                    .hits
                    .iter()
                    .map(|hit| (hit.logical_id, hit.score))
                    .collect();
                assert_eq!(
                    one_shot_rows, want_rows,
                    "shards={shards} rf={rf} scope={scope:?}: pit one-shot ≡ standalone"
                );
                assert_eq!(one_shot.total_hits, want.total_hits);

                for &size in &[1usize, 7] {
                    let pages = page_all(
                        &cluster,
                        pit,
                        title,
                        scope,
                        size,
                        &cluster_program,
                        one_shot.total_hits,
                    );
                    assert_eq!(
                        pages, want_rows,
                        "shards={shards} rf={rf} scope={scope:?} size={size}: page concat"
                    );
                }
            }

            // Live mutation between titles: remove a matched winner, add a
            // fresh query, upsert an existing one. The PIT above must not care
            // (asserted by the NEXT iterations still matching the frozen
            // reference), and a current-view read must see the changes.
            if i == 0 {
                let victim = reference
                    .try_match_title_top_k(
                        title,
                        options(1, QueryScope::WithBroad, None),
                        &reference_program,
                        &predicate,
                        &mut scratch,
                        None,
                    )
                    .expect("victim lookup")
                    .hits
                    .first()
                    .map(|hit| hit.logical_id);
                if let Some(victim) = victim {
                    cluster.remove_query(victim).expect("remove victim");
                    let live = cluster.percolate(title).expect("current-view percolate");
                    assert!(
                        !live.contains(&victim),
                        "current view must drop the removed winner"
                    );
                }
                cluster
                    .add_query(fresh_id, "jordan chrome refractor")
                    .expect("fresh add");
                fresh_id += 1;
            }
        }
        // After ALL the mutation above, the pit still serves the frozen world:
        // one final spot-check against the immutable reference.
        let title = &titles[0];
        let want = reference
            .try_match_title_top_k(
                title,
                options(1_000, QueryScope::WithBroad, None),
                &reference_program,
                &predicate,
                &mut scratch,
                None,
            )
            .expect("standalone");
        let pages = page_all(
            &cluster,
            pit,
            title,
            QueryScope::WithBroad,
            5,
            &cluster_program,
            want.total_hits,
        );
        assert_eq!(
            pages,
            want.hits
                .iter()
                .map(|hit| (hit.logical_id, hit.score))
                .collect::<Vec<_>>(),
            "shards={shards} rf={rf}: pit survives every mutation"
        );
        assert!(cluster.close_pit(pit), "close releases");
        assert!(!cluster.close_pit(pit), "second close is a no-op");
        assert_eq!(cluster.open_pit_count(), 0);
    }
}

/// Every invalidation shape is a typed fail-closed error, never a silent
/// current-view page: no-pit boundary, unknown/closed pit, TTL expiry, cap.
#[test]
fn boundary_and_lifecycle_failures_are_typed() {
    let (queries, _titles) = build_corpus();
    let tags = ranked_tags_parallel(&queries);
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster =
        ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags).expect("cluster build");
    let program = cluster
        .compile_rank_program(&rank_program())
        .expect("program");

    // A search_after boundary without a PIT is refused (it could mix
    // generations, and a remote wire could not even carry it).
    let err = cluster
        .try_percolate_filtered_top_k(
            "michael jordan",
            &[],
            options(5, QueryScope::Standard, Some((0, 0))),
            &program,
            None,
        )
        .expect_err("boundary without pit");
    assert!(matches!(
        err,
        ClusterRankedError::Shard(reverse_rusty::cluster::ShardError::PitUnsupported(_))
    ));

    // Unknown pit id ⇒ stale.
    let err = cluster
        .try_percolate_filtered_top_k_pit(
            reverse_rusty::PitId(999),
            "michael jordan",
            &[],
            options(5, QueryScope::Standard, None),
            &program,
            None,
            Instant::now(),
        )
        .expect_err("unknown pit");
    assert!(matches!(err, ClusterRankedError::StalePit));

    // TTL expiry (injected clock) ⇒ stale; the shard pins are released by the
    // lazy reap on the next PIT-API touch.
    let now = Instant::now();
    let pit = cluster
        .open_pit(Some(Duration::from_secs(5)), &PitConfig::default(), now)
        .expect("open");
    let later = now + Duration::from_secs(6);
    let err = cluster
        .try_percolate_filtered_top_k_pit(
            pit,
            "michael jordan",
            &[],
            options(5, QueryScope::Standard, None),
            &program,
            None,
            later,
        )
        .expect_err("expired pit");
    assert!(matches!(err, ClusterRankedError::StalePit));
    assert_eq!(cluster.open_pit_count(), 0, "reap released the entry");

    // Cap ⇒ typed admission; keep-alive ceiling ⇒ typed admission.
    let tiny = PitConfig {
        max_open: 1,
        ..PitConfig::default()
    };
    let _held = cluster.open_pit(None, &tiny, now).expect("first");
    let err = cluster.open_pit(None, &tiny, now).expect_err("cap");
    assert!(matches!(
        err,
        ClusterPitError::Admission(PitError::LimitExceeded { max: 1 })
    ));
    let err = cluster
        .open_pit(
            Some(Duration::from_secs(10_000)),
            &PitConfig::default(),
            now,
        )
        .expect_err("keep-alive ceiling");
    assert!(matches!(
        err,
        ClusterPitError::Admission(PitError::KeepAliveTooLarge { .. })
    ));
}

/// The three placement-invalidation shapes: `resize` and `set_vocab` (the two
/// placement-generation bumps) stale every open PIT, and a durable reopen
/// serves no prior PIT at all (the registry is in-memory by design).
#[test]
fn resize_set_vocab_and_reopen_stale_open_pits() {
    let (queries, _titles) = build_corpus();
    let tags = ranked_tags_parallel(&queries);

    // resize ⇒ StalePit.
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let mut cluster =
        ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags).expect("build");
    let program = cluster
        .compile_rank_program(&rank_program())
        .expect("program");
    let pit = cluster
        .open_pit(None, &PitConfig::default(), Instant::now())
        .expect("open");
    cluster.resize(5).expect("resize");
    let err = cluster
        .try_percolate_filtered_top_k_pit(
            pit,
            "michael jordan",
            &[],
            options(5, QueryScope::Standard, None),
            &program,
            None,
            Instant::now(),
        )
        .expect_err("post-resize page");
    assert!(matches!(err, ClusterRankedError::StalePit));
    assert_eq!(
        cluster.open_pit_count(),
        0,
        "rebuild cleared the dead entries"
    );
    // A pit opened AFTER the resize works — ids were not reused.
    let fresh = cluster
        .open_pit(None, &PitConfig::default(), Instant::now())
        .expect("post-resize open");
    assert_ne!(fresh, pit, "pit ids are never reused across a rebuild");
    cluster
        .try_percolate_filtered_top_k_pit(
            fresh,
            "michael jordan",
            &[],
            options(5, QueryScope::Standard, None),
            &program,
            None,
            Instant::now(),
        )
        .expect("fresh pit serves");

    // set_vocab ⇒ StalePit.
    let mut cluster =
        ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags).expect("build");
    let program = cluster
        .compile_rank_program(&rank_program())
        .expect("program");
    let pit = cluster
        .open_pit(None, &PitConfig::default(), Instant::now())
        .expect("open");
    cluster
        .set_vocab(reverse_rusty::vocab::Vocab::default())
        .expect("set_vocab");
    let err = cluster
        .try_percolate_filtered_top_k_pit(
            pit,
            "michael jordan",
            &[],
            options(5, QueryScope::Standard, None),
            &program,
            None,
            Instant::now(),
        )
        .expect_err("post-set_vocab page");
    assert!(matches!(err, ClusterRankedError::StalePit));

    // Durable reopen ⇒ StalePit (fresh in-memory registry).
    let dir = std::env::temp_dir().join(format!("rr-adr113-pit-reopen-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let cfg = ClusterConfig {
        num_shards: 3,
        include_broad: true,
        data_dir: Some(dir.clone()),
        ..ClusterConfig::default()
    };
    let cluster =
        ClusterEngine::build_with_tags(vocab(), &cfg, &queries, &tags).expect("durable build");
    let pit = cluster
        .open_pit(None, &PitConfig::default(), Instant::now())
        .expect("open");
    cluster.checkpoint().expect("checkpoint");
    drop(cluster);
    let cluster = ClusterEngine::open(&dir, vocab(), None).expect("reopen");
    let program = cluster
        .compile_rank_program(&rank_program())
        .expect("program");
    let err = cluster
        .try_percolate_filtered_top_k_pit(
            pit,
            "michael jordan",
            &[],
            options(5, QueryScope::Standard, None),
            &program,
            None,
            Instant::now(),
        )
        .expect_err("post-reopen page");
    assert!(matches!(err, ClusterRankedError::StalePit));
}
