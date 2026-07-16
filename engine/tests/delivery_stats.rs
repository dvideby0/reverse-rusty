//! ADR-107 delivery telemetry: exact-member emissions versus final logical IDs.

use reverse_rusty::cluster::{ClusterConfig, ClusterEngine};
use reverse_rusty::segment::{BatchMatchOptions, Engine, MatchScratch};
use reverse_rusty::Normalizer;

#[test]
fn scalar_and_batch_count_duplicates_before_logical_dedup() {
    let mut engine = Engine::new(Normalizer::default_vocab().expect("normalizer"));
    // Two live physical members with one logical identity. The compatibility
    // result stays one ID, while telemetry records both exact emissions.
    engine.insert_live("zzdelivery", 42, 1);
    engine.insert_live("zzdelivery", 42, 2);

    let mut scratch = MatchScratch::new();
    let mut ids = Vec::new();
    let scalar = engine.match_title("zzdelivery", &mut scratch, &mut ids, true);
    assert_eq!(ids, vec![42]);
    assert_eq!(scalar.matches, 1);
    assert_eq!(scalar.logical_emissions, 2);
    assert_eq!(scalar.duplicate_emissions, 1);

    let (batch, stats) = engine.match_titles_batch_with_stats(
        &["zzdelivery"],
        BatchMatchOptions {
            include_broad: true,
            ..BatchMatchOptions::default()
        },
    );
    assert_eq!(batch, vec![(0, vec![42])]);
    assert_eq!(stats.matches, 1);
    assert_eq!(stats.logical_emissions, 2);
    assert_eq!(stats.duplicate_emissions, 1);
}

#[test]
fn coordinator_adds_cross_shard_duplicates_without_double_counting_emissions() {
    let query = "(zzdela,zzdelb,zzdelc,zzdeld,zzdele,zzdelf,zzdelg,zzdelh)";
    let title = "zzdela zzdelb zzdelc zzdeld zzdele zzdelf zzdelg zzdelh";
    // Fill the frozen top-64 with earlier seed features so the target any-of
    // members remain selective and are placed on their distinct ring positions.
    let mut queries: Vec<(u64, String)> = (0..80)
        .map(|i| (i, format!("zzseed{i} commonbrand")))
        .collect();
    queries.push((1_000, query.to_string()));
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

    assert!(cluster.shard_fanout(title).len() > 1, "test must fan out");
    let (ids, stats) = cluster.percolate_with_stats(title).expect("percolate");
    assert_eq!(ids, vec![1_000]);
    assert_eq!(stats.matches, 1);
    assert!(
        stats.logical_emissions >= 2,
        "both placed copies should emit before coordinator dedup"
    );
    assert_eq!(
        stats.duplicate_emissions,
        stats.logical_emissions - 1,
        "every physical emission beyond the one logical result is duplicate delivery"
    );
}
