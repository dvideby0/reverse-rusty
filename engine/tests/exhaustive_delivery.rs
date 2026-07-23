use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
use reverse_rusty::config::EngineConfig;
use reverse_rusty::delivery::{
    ChunkSink, ChunkSinkError, DeliveryChecksum, ExhaustiveMatchError, ExhaustiveOptions,
    MatchChunk,
};
use reverse_rusty::exact::TagPredicate;
use reverse_rusty::gen::{gen_class_d_queries, generate, GenConfig};
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::{Normalizer, QueryScope};

#[derive(Default)]
struct RecordingSink {
    chunks: Vec<MatchChunk>,
    fail_at: Option<u64>,
}

impl ChunkSink for RecordingSink {
    fn send_chunk(&mut self, chunk: &MatchChunk) -> Result<(), ChunkSinkError> {
        if self.fail_at == Some(chunk.sequence) {
            return Err(ChunkSinkError::new("injected chunk failure"));
        }
        self.chunks.push(chunk.clone());
        Ok(())
    }
}

struct PollingCancelSink {
    polls: usize,
    cancel_at: usize,
    chunks: usize,
}

impl ChunkSink for PollingCancelSink {
    fn send_chunk(&mut self, _chunk: &MatchChunk) -> Result<(), ChunkSinkError> {
        self.chunks += 1;
        Ok(())
    }

    fn check_cancelled(&mut self) -> Result<(), ChunkSinkError> {
        self.polls += 1;
        if self.polls >= self.cancel_at {
            Err(ChunkSinkError::new("injected out-of-band cancellation"))
        } else {
            Ok(())
        }
    }
}

fn engine_with_physical_duplicates() -> Engine {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    engine
        .try_insert_live("michael jordan", 7, 1)
        .expect("first version");
    engine
        .try_insert_live("michael jordan", 8, 1)
        .expect("second id");
    engine.flush();
    // Deliberate legacy append, not upsert: logical 7 now has two live physical
    // copies in different segments with different bodies.
    engine
        .try_insert_live("lebron james", 7, 2)
        .expect("duplicate logical append");
    engine
}

#[test]
fn chunked_set_equals_compatibility_and_dedups_across_segments() {
    let engine = engine_with_physical_duplicates();
    let snapshot = engine.snapshot();
    let mut expected = Vec::new();
    snapshot.match_title(
        "michael jordan lebron james",
        &mut MatchScratch::new(),
        &mut expected,
        true,
    );

    let mut sink = RecordingSink::default();
    let result = snapshot
        .try_match_title_chunks(
            "michael jordan lebron james",
            ExhaustiveOptions {
                query_scope: QueryScope::WithBroad,
                chunk_size: 1,
            },
            None,
            &TagPredicate::empty(),
            &mut MatchScratch::new(),
            None,
            &mut sink,
        )
        .expect("chunked match");

    let delivered: Vec<u64> = sink
        .chunks
        .iter()
        .flat_map(|chunk| chunk.matches.iter().map(|member| member.logical_id))
        .collect();
    assert_eq!(delivered, expected);
    assert_eq!(result.summary.exact_total, expected.len() as u64);
    assert_eq!(result.summary.chunk_count, expected.len() as u64);
    assert_eq!(
        sink.chunks
            .iter()
            .map(|chunk| chunk.sequence)
            .collect::<Vec<_>>(),
        (0..expected.len() as u64).collect::<Vec<_>>()
    );

    let mut checksum = DeliveryChecksum::default();
    for member in sink.chunks.iter().flat_map(|chunk| &chunk.matches) {
        checksum.observe(*member);
    }
    assert_eq!(checksum, result.summary.checksum);
    assert_eq!(result.stats.duplicate_emissions, 1);
}

#[test]
fn older_matching_version_survives_when_newer_duplicate_does_not_match() {
    let engine = engine_with_physical_duplicates();
    let snapshot = engine.snapshot();
    let mut sink = RecordingSink::default();
    let result = snapshot
        .try_match_title_chunks(
            "michael jordan",
            ExhaustiveOptions {
                query_scope: QueryScope::Standard,
                chunk_size: 8,
            },
            None,
            &TagPredicate::empty(),
            &mut MatchScratch::new(),
            None,
            &mut sink,
        )
        .expect("chunked match");
    let ids: Vec<u64> = sink.chunks[0]
        .matches
        .iter()
        .map(|member| member.logical_id)
        .collect();
    assert_eq!(ids, vec![7, 8]);
    assert_eq!(result.summary.exact_total, 2);
}

#[test]
fn sink_failure_never_produces_a_terminal_summary() {
    let engine = engine_with_physical_duplicates();
    let snapshot = engine.snapshot();
    let mut sink = RecordingSink {
        chunks: Vec::new(),
        fail_at: Some(1),
    };
    let error = snapshot
        .try_match_title_chunks(
            "michael jordan lebron james",
            ExhaustiveOptions {
                query_scope: QueryScope::WithBroad,
                chunk_size: 1,
            },
            None,
            &TagPredicate::empty(),
            &mut MatchScratch::new(),
            None,
            &mut sink,
        )
        .expect_err("second chunk must fail");
    assert!(matches!(error, ExhaustiveMatchError::Sink(_)));
    assert_eq!(sink.chunks.len(), 1);
    assert_eq!(sink.chunks[0].sequence, 0);
}

#[test]
fn out_of_band_cancellation_stops_a_zero_chunk_match() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    for logical in 0..2_000 {
        engine
            .try_insert_live(
                &format!("cancelneedle -blocked -zzcancelunique{logical}"),
                logical,
                1,
            )
            .expect("insert");
    }
    let snapshot = engine.snapshot();
    let mut sink = PollingCancelSink {
        polls: 0,
        cancel_at: 128,
        chunks: 0,
    };
    let error = snapshot
        .try_match_title_chunks(
            "cancelneedle blocked",
            ExhaustiveOptions {
                query_scope: QueryScope::WithBroad,
                chunk_size: 512,
            },
            None,
            &TagPredicate::empty(),
            &mut MatchScratch::new(),
            None,
            &mut sink,
        )
        .expect_err("sink cancellation must abort before a chunk exists");
    assert!(matches!(error, ExhaustiveMatchError::Sink(_)));
    assert_eq!(sink.polls, 128);
    assert_eq!(sink.chunks, 0);
}

#[test]
fn cancellation_polls_every_dead_body_group_member() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    let mut locals = Vec::new();
    for logical in 0..2_048 {
        locals.push(
            engine
                .insert_live("zzdeadgroupcancel", logical, 1)
                .expect("deduplicated insert"),
        );
    }
    for local in locals {
        engine.tombstone(local).expect("tombstone");
    }

    let snapshot = engine.snapshot();
    let mut sink = PollingCancelSink {
        polls: 0,
        cancel_at: 32,
        chunks: 0,
    };
    let error = snapshot
        .try_match_title_chunks(
            "zzdeadgroupcancel",
            ExhaustiveOptions {
                query_scope: QueryScope::WithBroad,
                chunk_size: 512,
            },
            None,
            &TagPredicate::empty(),
            &mut MatchScratch::new(),
            None,
            &mut sink,
        )
        .expect_err("dead body-group walk must observe cancellation");
    assert!(matches!(error, ExhaustiveMatchError::Sink(_)));
    assert_eq!(
        sink.polls, 32,
        "cancellation must be polled inside the group, not after all members"
    );
    assert_eq!(sink.chunks, 0);
}

#[test]
fn every_chunk_boundary_is_fail_closed() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("vocab"));
    for logical in 0..6 {
        engine
            .try_insert_live("boundaryneedle", logical, 1)
            .expect("insert");
    }
    let snapshot = engine.snapshot();
    for boundary in 0..6 {
        let mut sink = RecordingSink {
            chunks: Vec::new(),
            fail_at: Some(boundary),
        };
        let error = snapshot
            .try_match_title_chunks(
                "boundaryneedle",
                ExhaustiveOptions {
                    query_scope: QueryScope::Standard,
                    chunk_size: 1,
                },
                None,
                &TagPredicate::empty(),
                &mut MatchScratch::new(),
                None,
                &mut sink,
            )
            .expect_err("injected boundary must fail");
        assert!(matches!(error, ExhaustiveMatchError::Sink(_)));
        assert_eq!(sink.chunks.len(), boundary as usize);
        assert_eq!(
            sink.chunks
                .iter()
                .map(|chunk| chunk.sequence)
                .collect::<Vec<_>>(),
            (0..boundary).collect::<Vec<_>>()
        );
    }
}

#[test]
fn cluster_resequences_ownership_disjoint_shards_without_materializing() {
    let mut queries: Vec<(u64, String)> = (0..80)
        .map(|i| (i, format!("zzseed{i} commonbrand")))
        .collect();
    queries.extend([
        (1_000, "zzdela".to_string()),
        (1_001, "zzdelb".to_string()),
        (1_002, "zzdelc".to_string()),
        (1_003, "zzdeld".to_string()),
    ]);
    let cluster = ClusterEngine::build(
        Normalizer::default_vocab().expect("normalizer"),
        &ClusterConfig {
            num_shards: 8,
            include_broad: true,
            ..ClusterConfig::default()
        },
        &queries,
    )
    .expect("cluster build");
    let title = "zzdela zzdelb zzdelc zzdeld";
    assert!(cluster.shard_fanout(title).len() > 1, "test must fan out");
    let expected = cluster
        .percolate_with_broad(title, true)
        .expect("compatibility result");

    let mut sink = RecordingSink::default();
    let result = cluster
        .try_percolate_filtered_all(title, &[], QueryScope::WithBroad, None, 2, None, &mut sink)
        .expect("cluster exhaustive result");
    let delivered: Vec<u64> = sink
        .chunks
        .iter()
        .flat_map(|chunk| chunk.matches.iter().map(|member| member.logical_id))
        .collect();
    let mut delivered_sorted = delivered.clone();
    delivered_sorted.sort_unstable();
    assert_eq!(delivered_sorted, expected);
    assert_eq!(result.summary.exact_total, expected.len() as u64);
    assert_eq!(result.summary.chunk_count, sink.chunks.len() as u64);
    assert_eq!(
        sink.chunks
            .iter()
            .map(|chunk| chunk.sequence)
            .collect::<Vec<_>>(),
        (0..sink.chunks.len() as u64).collect::<Vec<_>>()
    );
}

#[test]
fn exhaustive_equals_compatibility_across_all_cost_classes_and_scopes() {
    let mut data = generate(&GenConfig {
        // Match the established ADR-105 hot-tier oracle scale: at 5K this
        // seed can legitimately leave every non-top-64 anchor below theta.
        num_queries: 20_000,
        num_titles: 120,
        broad_query_frac: 0.08,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0407_7E57,
        num_players: 2_000,
        num_sets: 1_000,
    });
    let mut next = data.queries.len() as u64;
    data.queries
        .push((next, "(rareexhaustive1,rareexhaustive2)".into()));
    next += 1;
    for query in gen_class_d_queries(0xD114, 40) {
        data.queries.push((next, query));
        next += 1;
    }
    data.titles.push("rareexhaustive1".into());
    let mut engine = Engine::with_config(
        Normalizer::default_vocab().expect("normalizer"),
        EngineConfig {
            accept_class_d: true,
            hot_anchor_threshold: 64,
            ..EngineConfig::default()
        },
    );
    engine
        .try_build_from_queries(&data.queries)
        .expect("mixed build");
    let counts = engine.class_counts();
    for (index, class) in ["A", "B", "C", "D", "H"].into_iter().enumerate() {
        assert!(
            counts[index] > 0,
            "test corpus has no class {class}: {counts:?}"
        );
    }
    let snapshot = engine.snapshot();
    for scope in [QueryScope::Standard, QueryScope::WithBroad] {
        for title in &data.titles {
            let mut expected = Vec::new();
            snapshot.match_title(
                title,
                &mut MatchScratch::new(),
                &mut expected,
                scope == QueryScope::WithBroad,
            );
            let mut sink = RecordingSink::default();
            let result = snapshot
                .try_match_title_chunks(
                    title,
                    ExhaustiveOptions {
                        query_scope: scope,
                        chunk_size: 31,
                    },
                    None,
                    &TagPredicate::empty(),
                    &mut MatchScratch::new(),
                    None,
                    &mut sink,
                )
                .expect("exhaustive match");
            let mut delivered: Vec<u64> = sink
                .chunks
                .iter()
                .flat_map(|chunk| chunk.matches.iter().map(|member| member.logical_id))
                .collect();
            delivered.sort_unstable();
            assert_eq!(delivered, expected, "scope={scope:?}, title={title:?}");
            assert_eq!(result.summary.exact_total, expected.len() as u64);
        }
    }
}
