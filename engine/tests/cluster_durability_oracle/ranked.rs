//! ADR-110/112 reads reuse existing segment/source durability without a format
//! bump. The batch view runs post-flush, so this is also the mmap-backed
//! batch-kernel coverage (in-memory batch legs live in `cluster_oracle`).

use crate::harness::*;
use reverse_rusty::{QueryScope, RankProgramSpec, TopKOptions};

type RankedView = (
    Vec<(u64, i64)>,
    reverse_rusty::TotalHits,
    Vec<String>,
    Vec<Vec<(u64, i64)>>,
    Vec<Vec<String>>,
);

fn ranked_view(cluster: &ClusterEngine) -> RankedView {
    let program = cluster
        .compile_rank_program(&RankProgramSpec {
            priority_field: None,
            boosts: vec![
                ("tier".to_string(), "gold".to_string(), 50),
                ("tier".to_string(), "silver".to_string(), -7),
            ],
        })
        .expect("rank program");
    let options = TopKOptions {
        search_after: None,
        size: 7,
        track_total_hits_up_to: 10_000,
        query_scope: QueryScope::WithBroad,
    };
    let ranked = cluster
        .try_percolate_filtered_top_k("2020 topps chrome update", &[], options, &program, None)
        .expect("top k");
    let rows = ranked
        .hits
        .iter()
        .map(|hit| (hit.logical_id, hit.score))
        .collect();
    let sources = cluster
        .fetch_ranked_sources(&ranked, None)
        .expect("winner fetch");
    // The ADR-112 batch over the same durable segments: per-title rows must be
    // byte-stable across checkpoint/reopen/restore exactly like the scalar.
    let batch_titles = [
        "2020 topps chrome update".to_string(),
        "1998 topps chrome refractor".to_string(),
    ];
    let batch = cluster
        .try_percolate_filtered_top_k_batch(&batch_titles, &[], options, &program, None)
        .expect("batch top k");
    let batch_rows = batch
        .titles
        .iter()
        .map(|title| {
            title
                .hits
                .iter()
                .map(|hit| (hit.logical_id, hit.score))
                .collect()
        })
        .collect();
    let batch_sources = cluster
        .fetch_ranked_sources_batch_bounded(&batch, 16 * 1024 * 1024, None)
        .expect("batch winner fetch");
    (rows, ranked.total_hits, sources, batch_rows, batch_sources)
}

#[test]
fn top_k_and_winner_sources_survive_checkpoint_reopen_and_backup_restore() {
    let queries: Vec<(u64, String)> = (1..=30)
        .map(|id| (id, "topps chrome".to_string()))
        .collect();
    let tags: Vec<Vec<(String, String)>> = queries
        .iter()
        .map(|(id, _)| {
            vec![(
                "tier".to_string(),
                if id % 2 == 0 { "gold" } else { "silver" }.to_string(),
            )]
        })
        .collect();
    let dir = unique_dir("ranked_reopen");
    let backup = unique_dir("ranked_backup");
    let expected = {
        let cluster = ClusterEngine::build_with_tags(
            vocab(),
            &durable_cfg(3, dir.clone(), false),
            &queries,
            &tags,
        )
        .expect("durable tagged build");
        cluster.flush().expect("flush");
        cluster.checkpoint().expect("checkpoint");
        let expected = ranked_view(&cluster);
        cluster.backup_to(&backup).expect("backup");
        expected
    };

    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    assert_eq!(ranked_view(&reopened), expected);
    let restored = ClusterEngine::open(backup.clone(), vocab(), None).expect("restore backup");
    assert_eq!(ranked_view(&restored), expected);

    let _ = std::fs::remove_dir_all(dir);
    let _ = std::fs::remove_dir_all(backup);
}
