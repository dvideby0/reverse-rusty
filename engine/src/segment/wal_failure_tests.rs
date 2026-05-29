//! WAL-failure integration tests for the engine write path. Declared from the
//! `segment` module root under `#[cfg(test)]`.

use super::*;
use crate::config::EngineConfig;
use crate::error::WriteError;

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir =
        std::env::temp_dir().join(format!("percolator_segwal_{}_{}", name, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn engine_with_wal(name: &str) -> Engine {
    let cfg = EngineConfig {
        data_dir: Some(temp_dir(name)),
        ..EngineConfig::default()
    };
    Engine::with_config(Normalizer::default_vocab().expect("vocab"), cfg)
}

/// Micro-benchmark: cost of a single durable `bulk_ingest` as the base corpus
/// grows. P1-15 added a `sources.dat` rewrite to the bulk commit; the manifest
/// (which serializes the whole dict) was already O(corpus), so this shows the
/// per-call persistence cost stays in the same order. Ignored by default (it
/// does real disk writes). Run with:
///   cargo test --release --lib wal_failure_tests::bench_bulk_persist_cost -- --ignored --nocapture
#[test]
#[ignore = "benchmark: does real disk writes; run with --ignored"]
fn bench_bulk_persist_cost() {
    use crate::gen::{generate, GenConfig};
    use std::time::Instant;

    for &base_n in &[10_000usize, 50_000, 100_000] {
        let data = generate(&GenConfig {
            num_queries: base_n + 2_000,
            num_titles: 0,
            seed: 0xB017,
            ..GenConfig::default()
        });
        let mut eng = engine_with_wal(&format!("bulk_persist_{base_n}"));
        eng.build_from_queries(&data.queries[..base_n]);

        // Time a single small bulk_ingest into the now-large corpus.
        let batch: Vec<(u64, String)> = data.queries[base_n..base_n + 200].to_vec();
        let t = Instant::now();
        let report = eng
            .try_bulk_ingest(&batch)
            .expect("bulk ingest should be durable");
        let elapsed = t.elapsed();
        assert!(report.ingested > 0);
        println!(
            "base={base_n:>7} queries  bulk(200) durable commit: {:>7.2} ms  ({} total queries)",
            elapsed.as_secs_f64() * 1000.0,
            eng.num_queries(),
        );
    }
}

// P1-17: a WAL write failure must reject the insert (not apply it and report
// success). Verifies the in-memory state is untouched after the failure.
#[test]
fn wal_insert_failure_is_rejected_not_acknowledged() {
    let mut eng = engine_with_wal("insert_fail");
    assert!(matches!(
        eng.try_insert_live("michael jordan", 1, 1),
        Ok(InsertOutcome::Inserted(_))
    ));
    let before = eng.num_queries();
    assert!(eng.get_query_source(1).is_some());

    eng.wal.as_mut().unwrap().break_writes_for_test();
    let err = eng.try_insert_live("scottie pippen", 2, 1).unwrap_err();
    assert!(
        matches!(err, WriteError::Wal(_)),
        "expected Wal error, got {err:?}"
    );
    assert_eq!(
        eng.num_queries(),
        before,
        "rejected insert must not change the corpus"
    );
    assert!(
        eng.get_query_source(2).is_none(),
        "rejected insert must not be visible"
    );
    assert!(
        !eng.wal_healthy,
        "wal_healthy must flip to false after a failed append"
    );
}

// P1-17: a WAL write failure must reject the delete and leave the entry alive.
#[test]
fn wal_delete_failure_is_rejected_not_acknowledged() {
    let mut eng = engine_with_wal("delete_fail");
    eng.try_insert_live("michael jordan", 1, 1).unwrap();
    assert!(eng.get_query_source(1).is_some());

    eng.wal.as_mut().unwrap().break_writes_for_test();
    assert!(
        eng.delete_by_logical_id(1).is_err(),
        "delete must surface the WAL error"
    );
    assert!(
        eng.get_query_source(1).is_some(),
        "rejected delete must leave the entry alive"
    );
}

// A malformed query is a Parse error that never touches the WAL, so it is
// distinct from a durability failure and leaves the WAL healthy.
#[test]
fn parse_failure_is_distinct_from_durability_failure() {
    let mut eng = engine_with_wal("parse_vs_wal");
    let err = eng.try_insert_live("(", 9, 1).unwrap_err();
    assert!(matches!(err, WriteError::Parse(_)));
    assert!(
        eng.wal_healthy,
        "a parse failure must not mark the WAL unhealthy"
    );
}
