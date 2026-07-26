use super::*;

/// The C boundary is untouchable from both sides: a re-anchoring θ-on merge
/// keeps every class-C and class-B count intact (and broad-off results are
/// invariant through it — the extended demote guard's contract).
#[test]
fn compaction_never_moves_class_c_across() {
    let data = gen_corpus(0x0407_C0DE);
    let mut eng = build_multi(
        &data.queries,
        EngineConfig {
            hot_anchor_threshold: THETA,
            compaction_reanchor: true,
            auto_compact_on_flush: false,
            auto_compact_on_ingest: false,
            ..EngineConfig::default()
        },
    );
    let cc_before = eng.class_counts();
    assert!(cc_before[2] > 0, "degenerate: no class C to protect");
    assert!(cc_before[4] > 0, "degenerate: no class H in play");
    let sel_before = per_title_sets(&eng, &data.titles, false);
    let broad_before = per_title_sets(&eng, &data.titles, true);
    eng.flush();
    eng.compact_all().expect("compaction ran");
    let cc_after = eng.class_counts();
    assert_eq!(cc_after[2], cc_before[2], "class C count moved");
    assert_eq!(cc_after[3], cc_before[3], "class D count moved");
    assert_eq!(
        per_title_sets(&eng, &data.titles, false),
        sel_before,
        "broad-off results changed through a θ-on merge (a C-crossing FN)"
    );
    assert_eq!(
        per_title_sets(&eng, &data.titles, true),
        broad_before,
        "broad-on results changed through a θ-on merge"
    );
}

/// The messy-corpus pass (ADR-063 discipline): adversarial surface noise over
/// the θ-on engine still ≡ brute.
#[test]
fn messy_hot_corpus_differential() {
    let mut data = gen_corpus(0x0407_3E55);
    let mut rng = Rng::new(0x0407_3E55 ^ 0xA5A5);
    messify_dataset(&mut rng, &mut data, 0.8, 0.5);
    let eng = build_multi(&data.queries, cfg_theta(THETA));
    assert!(eng.class_counts()[4] > 0, "messy corpus lost its H mix");
    let brute = Brute::build(&data.queries);
    assert_no_fn_fp(
        &per_title_sets(&eng, &data.titles, true),
        &brute,
        &data.titles,
        "messy θ-on",
    );
}

/// Claim 5: a θ so high nothing classifies H leaves the hot tier structurally
/// free — identical probe counts to θ=0 (the skip-when-empty pin) and, of
/// course, identical results.
#[test]
fn hot_empty_is_free() {
    let data = gen_corpus(0x0407_F4EE);
    let eng_off = build_multi(&data.queries, cfg_theta(0));
    let eng_high = build_multi(&data.queries, cfg_theta(u32::MAX));
    assert_eq!(eng_high.class_counts()[4], 0);

    let mut s = MatchScratch::new();
    let mut out_a = Vec::new();
    let mut out_b = Vec::new();
    for include_broad in [false, true] {
        for t in data.titles.iter().take(200) {
            let st_off = eng_off.match_title(t, &mut s, &mut out_a, include_broad);
            let st_high = eng_high.match_title(t, &mut s, &mut out_b, include_broad);
            assert_eq!(out_a, out_b, "hot-empty engine diverged");
            assert_eq!(
                st_high.probes_attempted, st_off.probes_attempted,
                "an empty hot tier must add ZERO probes (include_broad={include_broad})"
            );
            assert_eq!(st_high.hot_postings_scanned, 0);
            assert_eq!(st_high.hot_candidates, 0);
        }
    }
}
