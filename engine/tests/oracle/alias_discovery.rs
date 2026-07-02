//! Distributional alias discovery (ADR-102) — the differential + governance oracle.
//!
//! The load-bearing claims: (1) `discover_aliases_and_record` changes **no** match results —
//! candidates are metadata, the install is metadata-only (no epoch bump, no recompile) — proven
//! differentially over a generated corpus; (2) an operator activating a discovered pair goes
//! through the proven ADR-054 expansion path — widening-only, the cross-form match appears and
//! nothing is lost; (3) discovery quality on a planted corpus: the substitute is proposed, the
//! co-listed any-of alternative is suppressed; (4) recorded candidates ride the vocab document —
//! the operator's actual single-node persistence path (`GET /_vocab` → reopen with it).

use reverse_rusty::config::EngineConfig;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty::vocab::{AliasStatus, DistributionalConfig};
use std::collections::HashSet;

fn matched(eng: &mut Engine, s: &mut MatchScratch, title: &str) -> HashSet<u64> {
    let mut out = Vec::new();
    eng.match_title(title, s, &mut out, true);
    out.iter().copied().collect()
}

/// A generated corpus salted with a planted substitute family (`zzud` / `zzupperdeck` filling
/// the same slot, never co-occurring) and a planted co-listed alternative (`zzpsa`,`zzbgs`
/// appearing together in any-ofs) — the two shapes the discoverer must separate.
fn salted_corpus() -> (Vec<(u64, String)>, Vec<String>) {
    let cfg = GenConfig {
        num_queries: 8_000,
        num_titles: 300,
        broad_query_frac: 0.05,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0xA1_1A5,
        num_players: 900,
        num_sets: 400,
    };
    let data = generate(&cfg);
    let mut queries = data.queries;
    let mut id = queries.iter().map(|(i, _)| *i).max().unwrap_or(0) + 1;
    for i in 0..40u64 {
        queries.push((id, format!("zzud ctxp{} ctxb{}", i % 7, i % 5)));
        id += 1;
        queries.push((id, format!("zzupperdeck ctxp{} ctxb{}", i % 7, i % 5)));
        id += 1;
        queries.push((id, format!("(zzpsa,zzbgs) ctxg{}", i % 7)));
        id += 1;
    }
    (queries, data.titles)
}

/// (1) The no-op differential: recording candidates changes NO match result on any title, and
/// the vocab epoch does not move (the metadata-only install took the fast path — this is the
/// recompile-skip soundness proof).
#[test]
fn discover_and_record_changes_no_match_results() {
    let (queries, titles) = salted_corpus();
    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    let mut s = MatchScratch::new();

    let before: Vec<HashSet<u64>> = titles
        .iter()
        .map(|t| matched(&mut eng, &mut s, t))
        .collect();
    let epoch_before = eng.vocab_epoch();

    let report = eng
        .discover_aliases_and_record(&DistributionalConfig::default())
        .expect("discover + record");
    assert!(
        report.new_candidates >= 1,
        "premise: the salted corpus must yield candidates; report: {report:?}"
    );
    assert_eq!(
        eng.vocab_epoch(),
        epoch_before,
        "candidate-only recording must take the metadata-only fast path (no epoch bump)"
    );
    assert_eq!(eng.stale_segment_count(), 0, "nothing marked stale");

    for (t, want) in titles.iter().zip(&before) {
        let got = matched(&mut eng, &mut s, t);
        assert_eq!(
            &got, want,
            "match results must be byte-identical after recording candidates (title: {t})"
        );
    }
}

/// (2) Operator activation of a discovered pair is FN-safe: the cross-form match appears
/// (a query saying `zzud` matches a `zzupperdeck` title) and no pre-activation match is lost
/// anywhere (expansion widens only — ADR-054).
#[test]
fn operator_activation_of_discovered_pair_is_fn_safe() {
    let (mut queries, titles) = salted_corpus();
    let probe_q = 9_900_001u64;
    queries.push((probe_q, "zzud ctxp0".into()));
    let cross_title = "zzupperdeck ctxp0 psa 10";

    let mut eng = Engine::new(Normalizer::default_vocab().expect("vocab"));
    eng.build_from_queries(&queries);
    let mut s = MatchScratch::new();

    eng.discover_aliases_and_record(&DistributionalConfig::default())
        .expect("discover + record");
    let forms = {
        let mut f = vec!["zzud".to_string(), "zzupperdeck".to_string()];
        f.sort();
        f
    };
    let entry = eng
        .aliases()
        .expect("vocab installed")
        .entries()
        .iter()
        .find(|e| e.forms == forms)
        .expect("the planted pair was recorded")
        .clone();
    assert_eq!(entry.status, AliasStatus::Candidate, "never auto-active");

    // Candidates are inert: the cross-form title does not match the probe query yet.
    let before_all: Vec<HashSet<u64>> = titles
        .iter()
        .map(|t| matched(&mut eng, &mut s, t))
        .collect();
    assert!(!matched(&mut eng, &mut s, cross_title).contains(&probe_q));

    // The operator flow: pull the vocab, activate the reviewed pair, PUT it back (set_vocab +
    // recompile — the genuine-change path).
    let mut vocab = eng.vocab().expect("vocab").clone();
    assert!(
        vocab.aliases_mut().activate(&forms),
        "activate the candidate"
    );
    eng.set_vocab(vocab).expect("set_vocab");
    eng.recompile_stale_segments();

    // The cross-form match appears…
    assert!(
        matched(&mut eng, &mut s, cross_title).contains(&probe_q),
        "after activation, the zzud query must match the zzupperdeck title"
    );
    // …and nothing was lost anywhere (widening-only).
    for (t, before) in titles.iter().zip(&before_all) {
        let after = matched(&mut eng, &mut s, t);
        assert!(
            after.is_superset(before),
            "activation must never lose a match (title: {t})"
        );
    }
}

/// (3) Discovery quality on the salted corpus: the substitute pair is proposed; the co-listed
/// any-of alternative is suppressed by the co-occurrence penalty.
#[test]
fn discovery_quality_on_salted_corpus() {
    let (queries, _titles) = salted_corpus();
    let pairs = reverse_rusty::vocab::discover_pairs(&queries, &DistributionalConfig::default());
    let has = |a: &str, b: &str| {
        pairs
            .iter()
            .any(|p| p.forms.contains(&a.to_string()) && p.forms.contains(&b.to_string()))
    };
    assert!(
        has("zzud", "zzupperdeck"),
        "the planted substitute must be proposed; got {pairs:?}"
    );
    assert!(
        !has("zzpsa", "zzbgs"),
        "the co-listed any-of alternative must be suppressed; got {pairs:?}"
    );
}

/// (4) Candidates ride the vocab document across a restart — the operator's actual single-node
/// persistence path: run discovery on a durable engine, capture the vocab (as `GET /_vocab`
/// would), reopen the data dir WITH that vocab (`open_with_vocab`, the `--vocab-file` path),
/// and the registry is intact.
#[test]
fn candidates_ride_the_vocab_document_across_reopen() {
    let (queries, titles) = salted_corpus();
    let dir = std::env::temp_dir().join(format!("rr_adr102_reopen_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let config = EngineConfig {
        data_dir: Some(dir.clone()),
        ..EngineConfig::default()
    };

    let (vocab_doc, before): (reverse_rusty::vocab::Vocab, Vec<HashSet<u64>>) = {
        let mut eng =
            Engine::with_config(Normalizer::default_vocab().expect("vocab"), config.clone());
        eng.build_from_queries(&queries);
        let report = eng
            .discover_aliases_and_record(&DistributionalConfig::default())
            .expect("discover + record");
        assert!(report.new_candidates >= 1);
        let mut s = MatchScratch::new();
        let before = titles
            .iter()
            .map(|t| matched(&mut eng, &mut s, t))
            .collect();
        (eng.vocab().expect("vocab").clone(), before)
        // drop = crash-free shutdown; segments + manifest are already durable
    };

    let mut reopened = Engine::open_with_vocab(vocab_doc, config).expect("reopen with vocab");
    let reg = reopened.aliases().expect("vocab installed");
    assert!(
        reg.entries()
            .iter()
            .any(|e| e.status == AliasStatus::Candidate),
        "recorded candidates survive via the vocab document"
    );
    let mut s = MatchScratch::new();
    for (t, want) in titles.iter().zip(&before) {
        assert_eq!(
            matched(&mut reopened, &mut s, t),
            *want,
            "reopened matching is byte-identical (title: {t})"
        );
    }
    let _ = std::fs::remove_dir_all(&dir);
}
