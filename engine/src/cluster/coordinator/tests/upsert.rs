use super::*;

/// Cluster upsert (ADR-070): a fresh id creates (`removed == 0`), a re-upsert replaces —
/// the OLD version stops matching, the NEW one matches, and exactly one live physical
/// copy remains (no additive duplicate, the pre-ADR-067 hazard at the cluster).
#[test]
fn upsert_creates_then_replaces_by_logical_id() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![
        (1u64, "1994 acme".to_string()),
        (2u64, "1995 vertex".to_string()),
    ];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster builds");

    // Create: a fresh id reports zero prior copies removed.
    let (removed, outcome) = cluster.upsert_query(3, "1996 vertex", 1).expect("upsert");
    assert_eq!(removed, 0, "fresh id ⇒ created");
    assert!(matches!(
        outcome,
        AddOutcome::Placed { .. } | AddOutcome::Replicated
    ));
    assert!(cluster.percolate("1996 vertex").expect("p").contains(&3));

    // Replace: the new version matches, the old does not — old-stops-matching IS the
    // no-additive-duplicate proof (the pre-ADR-067 hazard was both versions live at
    // once). Entry counts grow by design (tombstone + insert), so they are not asserted.
    let (removed, _) = cluster
        .upsert_query(3, "1997 metal universe", 1)
        .expect("upsert");
    assert!(removed > 0, "prior copy tombstoned ⇒ updated");
    assert!(
        !cluster.percolate("1996 vertex").expect("p").contains(&3),
        "old version must stop matching after replace"
    );
    assert!(
        cluster
            .percolate("1997 metal universe")
            .expect("p")
            .contains(&3),
        "new version must match after replace"
    );

    // Replace back: repeated upserts keep converging (no stale copy resurfaces).
    let (removed, _) = cluster.upsert_query(3, "1996 vertex", 1).expect("upsert");
    assert!(removed > 0);
    assert!(cluster.percolate("1996 vertex").expect("p").contains(&3));
    assert!(
        !cluster
            .percolate("1997 metal universe")
            .expect("p")
            .contains(&3),
        "replaced-away version must not resurface"
    );
}

/// A rejected NEW version never deletes the prior one (ADR-067 parity at the cluster):
/// a class-D (negation-only) upsert and a parse-error upsert both leave the stored
/// version live and matchable.
#[test]
fn upsert_rejection_keeps_prior_version_live() {
    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![(1u64, "1994 acme".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster builds");
    assert!(cluster.percolate("1994 acme").expect("p").contains(&1));

    // Class D: negation-only — rejected at placement, stored nowhere, deletes nothing.
    let (removed, outcome) = cluster.upsert_query(1, "-junk", 1).expect("upsert");
    assert_eq!(removed, 0, "a failed replace never deletes");
    assert!(matches!(outcome, AddOutcome::RejectedClassD));
    assert!(
        cluster.percolate("1994 acme").expect("p").contains(&1),
        "prior version stays matchable after a class-D upsert"
    );

    // Parse error: rejected before logging, deletes nothing.
    let (removed, outcome) = cluster.upsert_query(1, "(((", 1).expect("upsert");
    assert_eq!(removed, 0);
    assert!(matches!(outcome, AddOutcome::RejectedParse(_)));
    assert!(
        cluster.percolate("1994 acme").expect("p").contains(&1),
        "prior version stays matchable after a parse-error upsert"
    );
}

/// WAL-first fail-closed for upsert, mirroring `add_query_is_fail_closed_when_log_append_fails`:
/// when the durable log append fails the upsert is rejected whole — the prior version
/// remains live and matchable (the replace never half-applies).
#[test]
fn upsert_is_fail_closed_when_log_append_fails() {
    let dir = scratch_dir("upsert_failclosed");
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let seed = vec![(7u64, "1994 acme".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("durable cluster builds");

    cluster.log.break_writes_for_test();
    let res = cluster.upsert_query(7, "1995 vertex", 1);
    assert!(
        matches!(res, Err(ShardError::Log(_))),
        "expected Log error, got {res:?}"
    );
    assert!(
        cluster.percolate("1994 acme").expect("p").contains(&7),
        "prior version must remain matchable after a rejected upsert"
    );
    assert!(
        !cluster.percolate("1995 vertex").expect("p").contains(&7),
        "the rejected new version must not be matchable"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// B2: a cluster `PUT /_doc/{id} {"version":N}` must STORE version N, not the
/// hardcoded 1 — matching single-node `try_upsert_live_with_tags`. The version
/// rides the `ClusterMutation::Upsert` log frame (the durable, replayed-on-reopen
/// source of truth), so asserting the logged frame's version is the faithful
/// round-trip check. Needs private `log` access, so it lives in-module.
#[test]
fn upsert_threads_request_version_into_the_log_frame() {
    let dir = scratch_dir("upsert_version");
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let seed = vec![(1u64, "1994 acme".to_string())];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("durable cluster builds");

    // Upsert id 5 at a non-default version.
    let (_removed, outcome) = cluster
        .upsert_query(5, "1995 vertex", 42)
        .expect("versioned upsert");
    assert!(
        matches!(outcome, AddOutcome::Placed { .. } | AddOutcome::Replicated),
        "in-vocabulary upsert is accepted, got {outcome:?}"
    );

    // The logged Upsert frame must carry version 42 (NOT the old hardcoded 1).
    let replay = cluster.log.replay(LogPos(0)).expect("replay clog");
    let logged_version = replay.entries.iter().find_map(|(_, m)| match m {
        ClusterMutation::Upsert {
            logical: 5,
            version,
            ..
        } => Some(*version),
        _ => None,
    });
    assert_eq!(
        logged_version,
        Some(42),
        "cluster upsert must log the request version, not the hardcoded 1"
    );

    // And the default still logs version 1 (the byte-identical RF=1 path) for a fresh id.
    cluster
        .upsert_query(6, "1994 acme", 1)
        .expect("default-version upsert");
    let replay = cluster.log.replay(LogPos(0)).expect("replay clog");
    let default_version = replay.entries.iter().find_map(|(_, m)| match m {
        ClusterMutation::Upsert {
            logical: 6,
            version,
            ..
        } => Some(*version),
        _ => None,
    });
    assert_eq!(default_version, Some(1), "default upsert version stays 1");

    let _ = std::fs::remove_dir_all(&dir);
}

/// B2 follow-up (codex review): a blue/green rebuild (`set_vocab` / resize) must PRESERVE
/// each query's stored version rather than reset it to 1. Before the fix the rebuild gather
/// dropped the version and `rebuild_from_live` recreated every `PlacedQuery` with
/// `version: 1`, so a `PUT {"version":42}` was silently rewritten to 1 (and the checkpoint
/// truncated the original log frame — durable divergence from single-node). Asserts the
/// gather carries the stored version across the rebuild.
#[test]
fn rebuild_preserves_stored_query_version() {
    let dir = scratch_dir("rebuild_version");
    let cfg = ClusterConfig {
        num_shards: 3,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let seed = vec![(1u64, "1994 acme".to_string())];
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("durable cluster builds");

    // Upsert id 5 at a non-default version, then confirm the gather sees version 42.
    cluster
        .upsert_query(5, "1995 vertex", 42)
        .expect("versioned upsert");
    let before = cluster.live_corpus_tagged().expect("gather");
    let pre = before
        .iter()
        .find(|(l, ..)| *l == 5)
        .map(|(_, _, v, _, _, _, _, _)| *v);
    assert_eq!(
        pre,
        Some(42),
        "gather must see the stored version before rebuild"
    );

    // A vocabulary change forces a blue/green rebuild of every shard.
    let mut new_vocab = crate::vocab::Vocab::new();
    new_vocab.add_synonym("pkg", "term:new", crate::dict::FeatureKind::Category);
    cluster.set_vocab(new_vocab).expect("set_vocab rebuild");

    // After the rebuild id 5 must STILL carry version 42 (not reset to 1) and still match.
    let after = cluster.live_corpus_tagged().expect("gather after rebuild");
    let post = after
        .iter()
        .find(|(l, ..)| *l == 5)
        .map(|(_, _, v, _, _, _, _, _)| *v);
    assert_eq!(
        post,
        Some(42),
        "rebuild must preserve the stored version, not reset it to 1"
    );
    assert!(
        cluster
            .percolate("1995 vertex")
            .expect("percolate")
            .contains(&5),
        "the re-placed query must still match after the rebuild"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn legacy_tail_is_folded_before_current_placement_validation() {
    let dir = scratch_dir("legacy_tail_placement");
    let cfg = ClusterConfig {
        num_shards: 16,
        data_dir: Some(dir.clone()),
        ..Default::default()
    };
    let mut alias_vocab = crate::vocab::Vocab::new();
    alias_vocab
        .import_solr_aliases("ny => new york", &vocab(), &Dict::new())
        .expect("valid aliases");
    let cluster =
        ClusterEngine::build_with_vocab(alias_vocab, &cfg, &[]).expect("empty durable cluster");

    // Reconstruct the pre-ADR-118 lowering for `new -used york`: the old
    // compiler joined both positive bare terms across the intervening negated
    // clause, so `new york` collapsed as one query-side alias entity.
    let mut lc = String::new();
    let mut legacy = crate::compile::Extracted {
        required: cluster
            .norm
            .compile_features_readonly("new york", &cluster.dict, &mut lc),
        forbidden: cluster
            .norm
            .compile_features_readonly("used", &cluster.dict, &mut lc),
        anyof: Vec::new(),
        anyof_predicates: Vec::new(),
        forbidden_conjunctions: Vec::new(),
        required_phrases: Vec::new(),
        forbidden_phrases: Vec::new(),
    };
    legacy.required.sort_unstable();
    legacy.required.dedup();
    legacy.forbidden.sort_unstable();
    legacy.forbidden.dedup();
    legacy.expand_equivalences(cluster.dict.equivalences());

    let ast = crate::dsl::parse("new -used york").expect("current query");
    let current = crate::compile::extract_readonly(&ast, &cluster.norm, &cluster.dict, &mut lc);
    let generation = cluster.placement_generation();
    let legacy_placement = placement_of(
        &cluster.dict,
        &cluster.ring,
        &legacy,
        true,
        cluster.per_shard.hot_anchor_threshold,
    )
    .placement(generation, cfg.num_shards as u32)
    .expect("legacy placement");
    let current_placement = placement_of(
        &cluster.dict,
        &cluster.ring,
        &current,
        true,
        cluster.per_shard.hot_anchor_threshold,
    )
    .placement(generation, cfg.num_shards as u32)
    .expect("current placement");
    assert_ne!(
        legacy_placement, current_placement,
        "precondition: this tail must exercise a real placement change"
    );

    cluster
        .log
        .append(&ClusterMutation::Add {
            logical: 1,
            version: 1,
            dsl: "new -used york".into(),
            tags: Vec::new(),
            placement: legacy_placement,
        })
        .expect("append legacy-shaped tail");
    drop(cluster);

    let manifest_path = dir.join(CLUSTER_MANIFEST_FILE);
    downgrade_cluster_manifest_to_v6(&manifest_path);
    let reopened = ClusterEngine::open(&dir, vocab(), Some(&cfg))
        .expect("legacy tail is folded before re-placement");
    assert!(reopened
        .percolate("new vintage product york")
        .expect("match")
        .contains(&1));
    let _ = std::fs::remove_dir_all(&dir);
}

/// Tier-D: a degenerate same-node handoff (`from == to`) is a silent no-op — it must NOT
/// fence the source then flip routing onto itself. The `from == to` guard sits before the
/// handle resolve, so the self-handoff returns immediately, emitting no event and never
/// touching a shard. Asserted via the observer (no event) + percolate-unchanged. Gated:
/// `drive_autoscaled_handoff` only exists under `distributed`.
#[cfg(feature = "distributed")]
#[test]
fn self_handoff_is_skipped_without_fencing() {
    use crate::cluster::autoscale::LoadSnapshot;
    use crate::cluster::control::{NodeDescriptor, NodeId, NodeRole, ShardAssignment};

    let cfg = ClusterConfig {
        num_shards: 3,
        ..Default::default()
    };
    let seed = vec![
        (1u64, "1994 acme".to_string()),
        (2u64, "1995 vertex".to_string()),
    ];
    let cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster builds");

    // Record any emitted event — a real handoff (or its abort) emits a DurabilityFailure.
    let events: Arc<Mutex<Vec<EngineEvent>>> = Arc::new(Mutex::new(Vec::new()));
    {
        let sink = Arc::clone(&events);
        cluster.set_observer(Arc::new(move |ev: &EngineEvent| {
            sink.lock().unwrap().push(ev.clone());
        }));
    }

    let before = cluster.percolate("1994 acme").expect("percolate");

    // A snapshot where node 7 owns position 0 — and a Handoff that moves it from node 7 to
    // node 7 (the same node, same endpoint). The guard must short-circuit this.
    let node = NodeDescriptor {
        id: NodeId(7),
        addr: Some("http://127.0.0.1:65530".to_string()),
        role: NodeRole::Data,
    };
    let snapshot = LoadSnapshot {
        nodes: vec![node],
        assignments: vec![ShardAssignment {
            position: 0,
            primary: NodeId(7),
            replicas: Vec::new(),
        }],
        shard_corpus: vec![1, 1, 0],
        replicated_corpus: 0,
        num_shards: 3,
        replication_factor: 1,
    };

    cluster.drive_autoscaled_handoff(&snapshot, 0, NodeId(7), NodeId(7));

    assert!(
        events.lock().unwrap().is_empty(),
        "a self-handoff must emit no event (no fence, no abort): {:?}",
        events.lock().unwrap()
    );
    assert_eq!(
        cluster.percolate("1994 acme").expect("percolate"),
        before,
        "matching must be byte-identical across a skipped self-handoff"
    );
}

/// A blue/green rebuild (resize / set_vocab) re-ingests ALREADY-STORED queries through
/// `ingest_extracted`, carrying their tags as pre-resolved `TagId`s. Tightening `max_tags`
/// after those queries were accepted must NOT drop them on the rebuild — the rebuild swaps
/// in the new shards and ignores the ingest report, so a skipped query is permanently lost
/// (a false negative on acknowledged data). The `max_tags` cap applies only to FRESH raw-tag
/// ingestion, never to stored carry-through (codex review).
#[test]
fn rebuild_preserves_stored_tags_under_tightened_max_tags() {
    let mut per_shard = EngineConfig {
        max_tags: 5,
        ..EngineConfig::default()
    };
    per_shard.data_dir = None;
    let cfg = ClusterConfig {
        num_shards: 3,
        per_shard,
        ..Default::default()
    };
    // Seed so the dict knows the tokens, then add a query carrying 4 tags (≤ 5).
    let seed = vec![(1u64, "1994 acme appliance".to_string())];
    let mut cluster = ClusterEngine::build(vocab(), &cfg, &seed).expect("cluster builds");
    let four_tags: Vec<(String, String)> = (0..4).map(|i| ("k".into(), format!("v{i}"))).collect();
    cluster
        .add_query_with_tags(2, "1995 vertex appliance", &four_tags)
        .expect("tagged add");
    // The tagged query is matchable and filterable by one of its tags before the rebuild.
    let filter = vec![("k".to_string(), vec!["v3".to_string()])];
    assert!(cluster
        .percolate_filtered("1995 vertex appliance", &filter)
        .expect("filtered")
        .contains(&2));

    // Tighten the per-shard tag ceiling BELOW the stored query's 4 tags, then rebuild
    // (a resize triggers `rebuild_from_live` → `ingest_extracted` with the carry-through).
    cluster.per_shard.max_tags = 2;
    let rebuilt = cluster.resize(5).expect("resize rebuilds");
    assert_eq!(rebuilt, 2, "both stored queries are re-ingested");

    // The 4-tag query SURVIVES the rebuild: still matchable AND still filterable by its tag.
    assert!(
        cluster
            .percolate("1995 vertex appliance")
            .expect("p")
            .contains(&2),
        "stored over-limit-tagged query must survive the rebuild (no silent drop)"
    );
    assert!(
        cluster
            .percolate_filtered("1995 vertex appliance", &filter)
            .expect("filtered")
            .contains(&2),
        "the stored tags must survive carry-through — filter still matches"
    );

    // A FRESH add still respects the now-tightened cap: 3 raw tags > max_tags(2) is rejected.
    let three_tags: Vec<(String, String)> = (0..3).map(|i| ("k".into(), format!("w{i}"))).collect();
    let outcome = cluster
        .add_query_with_tags(9, "1996 vertex appliance", &three_tags)
        .expect("add returns");
    assert!(
        matches!(outcome, AddOutcome::RejectedParse(ref e) if e.kind == crate::error::ParseErrorKind::TooManyTags),
        "a fresh over-limit raw-tag add must still be rejected, got {outcome:?}"
    );
}
