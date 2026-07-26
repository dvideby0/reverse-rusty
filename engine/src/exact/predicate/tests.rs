use super::*;
use crate::compile::{AnyOfMember, AnyOfPredicate};
use crate::normalize::{PhraseArc, PhraseGraph, PositionArc};

fn extracted() -> Extracted {
    Extracted {
        required: vec![],
        forbidden: vec![],
        anyof: vec![vec![1, 3]],
        anyof_predicates: vec![AnyOfPredicate {
            members: vec![
                AnyOfMember {
                    requirements: vec![vec![1], vec![2]],
                },
                AnyOfMember {
                    requirements: vec![vec![3]],
                },
            ],
        }],
        forbidden_conjunctions: vec![vec![4, 5]],
        ..Extracted::default()
    }
}

#[test]
fn program_validates_and_preserves_member_boundaries() {
    let mut blob = Vec::new();
    let (off, len) = encode_predicate(&extracted(), &mut blob);
    let words = &blob[off as usize..off as usize + len as usize];
    validate_predicate(words).expect("valid program");

    let matches =
        |pos: &[u32], neg: &[u32]| verify_predicate(words, &TitleView::dual(0, pos, 0, neg));
    assert!(matches(&[1, 2], &[]));
    assert!(matches(&[3], &[]));
    assert!(!matches(&[1], &[]));
    assert!(matches(&[3], &[4]));
    assert!(!matches(&[3], &[4, 5]));
}

#[test]
fn malformed_program_fails_validation() {
    assert!(validate_predicate(&[99, 0, 0]).is_err());
    assert!(validate_predicate(&[FEATURE_PROGRAM_VERSION, 1, 1, 1, 2, 7]).is_err());
    assert!(validate_predicate(&[FEATURE_PROGRAM_VERSION, 0, 1, 1, 7]).is_err());
}

#[test]
fn phrase_program_preserves_required_and_forbidden_adjacency() {
    let ex = Extracted {
        required_phrases: vec![PhraseGraph {
            positions: 2,
            arcs: vec![
                PhraseArc {
                    start: 0,
                    end: 1,
                    alternatives: vec![1, 10],
                },
                PhraseArc {
                    start: 1,
                    end: 2,
                    alternatives: vec![2],
                },
            ],
        }],
        forbidden_phrases: vec![PhraseGraph {
            positions: 2,
            arcs: vec![
                PhraseArc {
                    start: 0,
                    end: 1,
                    alternatives: vec![4],
                },
                PhraseArc {
                    start: 1,
                    end: 2,
                    alternatives: vec![5],
                },
            ],
        }],
        ..Extracted::default()
    };
    let mut blob = Vec::new();
    let (off, len) = encode_predicate(&ex, &mut blob);
    let words = &blob[off as usize..off as usize + len as usize];
    validate_predicate(words).expect("valid phrase program");
    assert!(predicate_has_phrases(words));

    let verify = |positions: u32, arcs: &[PositionArc]| {
        let scratch = std::cell::RefCell::new(crate::exact::PhraseMatchScratch::default());
        verify_predicate(
            words,
            &TitleView::dual_positioned(
                &[],
                0,
                &[],
                positions,
                arcs,
                true,
                0,
                &[],
                positions,
                arcs,
                &scratch,
            ),
        )
    };
    assert!(verify(
        2,
        &[
            PositionArc {
                feature: 1,
                start: 0,
                end: 1,
            },
            PositionArc {
                feature: 2,
                start: 1,
                end: 2,
            },
        ],
    ));
    assert!(!verify(
        3,
        &[
            PositionArc {
                feature: 1,
                start: 0,
                end: 1,
            },
            PositionArc {
                feature: 9,
                start: 1,
                end: 2,
            },
            PositionArc {
                feature: 2,
                start: 2,
                end: 3,
            },
        ],
    ));
    assert!(!verify(
        4,
        &[
            PositionArc {
                feature: 10,
                start: 0,
                end: 1,
            },
            PositionArc {
                feature: 2,
                start: 1,
                end: 2,
            },
            PositionArc {
                feature: 4,
                start: 2,
                end: 3,
            },
            PositionArc {
                feature: 5,
                start: 3,
                end: 4,
            },
        ],
    ));
}

#[test]
fn malformed_phrase_graphs_fail_validation() {
    assert!(
        validate_predicate(&[PHRASE_PROGRAM_VERSION, 0, 0, 0, 0]).is_err(),
        "program v2 must carry at least one quoted graph"
    );
    assert!(validate_predicate(&[PHRASE_PROGRAM_VERSION, 0, 0, 1, 0, 0, 0]).is_err());
    assert!(
        validate_predicate(&[PHRASE_PROGRAM_VERSION, 0, 0, 1, 2, 1, 0, 1, 1, 7, 0,]).is_err(),
        "a graph without a path to its final position is invalid"
    );
    assert!(
        validate_predicate(&[PHRASE_PROGRAM_VERSION, 0, 0, 1, 1, 1, 0, 1, 2, 9, 7, 0,]).is_err(),
        "labels must be strictly canonical"
    );
}

#[test]
fn phrase_intersection_charges_arc_scans_to_the_fail_open_budget() {
    let positions = 300u32;
    let query_arcs: Vec<PhraseArc> = (1..=positions)
        .map(|end| PhraseArc {
            start: 0,
            end,
            alternatives: vec![1_000 + end],
        })
        .collect();
    let graph = PhraseGraph {
        positions,
        arcs: query_arcs,
    };
    let mut query = vec![graph.positions, graph.arcs.len() as u32];
    for arc in &graph.arcs {
        query.extend_from_slice(&[arc.start, arc.end, arc.alternatives.len() as u32]);
        query.extend_from_slice(&arc.alternatives);
    }
    let title_arcs: Vec<PositionArc> = (0..positions)
        .map(|start| PositionArc {
            feature: 7,
            start,
            end: start + 1,
        })
        .collect();

    let mut scratch = crate::exact::PhraseMatchScratch::default();
    assert_eq!(
        graph_matches(
            &query,
            PositionGraph {
                positions,
                arcs: &title_arcs,
                complete: true,
            },
            &mut scratch,
        ),
        None,
        "repeated query/title arc scans must exhaust a bounded work budget"
    );
    assert_eq!(
        crate::normalize::phrase_graph_matches_bounded(&graph, positions, &title_arcs, true,),
        None,
        "explain and the hot verifier must share the same work fail-open"
    );
}
