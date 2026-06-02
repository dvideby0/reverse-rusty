//! Control-plane seam oracle — the acceptance gate for clustering step 5a (ADR-037).
//!
//! The existing `cluster_oracle.rs` already proves cluster ≡ single-node ≡ brute across
//! shard counts × replication × broad. ADR-037 adds a `ControlPlane` seam + a default
//! single-node [`InMemoryControlPlane`] to the coordinator; this file proves the things
//! that introduces:
//!   * the default control plane PERTURBS NOTHING (cluster ≡ the independent brute oracle),
//!   * the committed cluster-state document is well-formed for an in-process build,
//!   * a shard→node REASSIGNMENT preserves correctness (zero false negatives across a
//!     map change — physical movement is a later increment),
//!   * every `ControlPlane` backend driven by the same script converges to the identical
//!     committed document (the `NullClusterLog ≡ FileClusterLog` differential pattern,
//!     structured so the openraft backend slots in as a second entry in step 5b).
//!
//! The document-mutation invariants + the fail-closed contract are unit-tested inside
//! `control.rs` (they need the `#[cfg(test)]`-only fault injector, which is unavailable to
//! an integration-test crate).

use reverse_rusty::cluster::{
    ClusterConfig, ClusterEngine, ClusterState, ClusterStateChange, ControlPlane,
    InMemoryControlPlane, NodeDescriptor, NodeId, NodeRole, ShardAssignment,
};
use reverse_rusty::compile::{extract, Extracted};
use reverse_rusty::dict::Dict;
use reverse_rusty::gen::{generate, GenConfig, BRANDS};
use reverse_rusty::normalize::Normalizer;
use std::collections::HashSet;

fn vocab() -> Normalizer {
    Normalizer::default_vocab().expect("built-in vocab")
}

/// Independent ground-truth matcher (same structure as `cluster_oracle.rs` / `oracle.rs` —
/// its own Dict/Normalizer, so it cannot share a bug with the engine or the cluster).
struct Brute {
    norm: Normalizer,
    dict: Dict,
    queries: Vec<(u64, Extracted)>,
}

impl Brute {
    fn build(queries: &[(u64, String)]) -> Self {
        let norm = vocab();
        let mut dict = Dict::new();
        let mut lc = String::new();
        let mut qs = Vec::new();
        for (logical, text) in queries {
            if let Ok(ast) = reverse_rusty::dsl::parse(text) {
                let ex = extract(&ast, &norm, &mut dict, &mut lc);
                if ex.required.is_empty() && ex.anyof.is_empty() {
                    continue; // mirror class-D rejection
                }
                qs.push((*logical, ex));
            }
        }
        dict.finalize_mask();
        Brute {
            norm,
            dict,
            queries: qs,
        }
    }

    fn matches(&self, title: &str, lc: &mut String, feats: &mut Vec<u32>) -> HashSet<u64> {
        self.norm.match_features(title, &self.dict, lc, feats);
        let present = |f: u32| feats.binary_search(&f).is_ok();
        let mut out = HashSet::new();
        for (logical, ex) in &self.queries {
            if ex.required.iter().all(|&f| present(f))
                && !ex.forbidden.iter().any(|&f| present(f))
                && ex.anyof.iter().all(|g| g.iter().any(|&f| present(f)))
            {
                out.insert(*logical);
            }
        }
        out
    }
}

/// Build the test corpus (identical to `cluster_oracle.rs`): a generated base (class A + C)
/// plus injected class-B any-of / arity-2 coverage and multi-entity titles.
fn build_corpus() -> (Vec<(u64, String)>, Vec<String>) {
    let cfg = GenConfig {
        num_queries: 12_000,
        num_titles: 1_200,
        broad_query_frac: 0.06,
        hot_skew: 2.0,
        family_size: 8,
        seed: 0x0CEA_5ADE,
        num_players: 2_000,
        num_sets: 800,
    };
    let data = generate(&cfg);
    let mut queries = data.queries;
    let mut titles = data.titles;
    let mut next_id = queries.iter().map(|(id, _)| *id).max().unwrap_or(0) + 1;

    for i in 0..150u64 {
        queries.push((next_id, format!("(rareplayer{i},rareplayer{})", i + 1000)));
        next_id += 1;
    }
    for i in 0..100u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        queries.push((next_id, format!("{year} {brand}")));
        next_id += 1;
    }
    for i in 0..150u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        queries.push((next_id, format!("{year} {brand} rareplayer{i}")));
        next_id += 1;
    }
    for i in 0..200u64 {
        let year = 1986 + (i % 39);
        let brand = BRANDS[(i % BRANDS.len() as u64) as usize];
        let a = i % 150;
        titles.push(format!(
            "{year} {brand} rareplayer{a} rareplayer{} psa 10",
            a + 1000
        ));
    }

    (queries, titles)
}

/// The default (single-node) control plane perturbs no matching, and the committed
/// cluster-state document is well-formed: one logical node owning every position.
#[test]
fn default_control_plane_is_well_formed_and_answer_invariant() {
    let (queries, titles) = build_corpus();
    let brute = Brute::build(&queries);

    let mut blc = String::new();
    let mut bfeats = Vec::new();
    let oracle: Vec<HashSet<u64>> = titles
        .iter()
        .map(|t| brute.matches(t, &mut blc, &mut bfeats))
        .collect();
    assert!(
        oracle.iter().map(HashSet::len).sum::<usize>() > 0,
        "degenerate corpus: no matches at all"
    );

    for &(k, rf) in &[(1usize, 1usize), (8, 1), (8, 2)] {
        let cfg = ClusterConfig {
            num_shards: k,
            replication_factor: rf,
            include_broad: true,
            ..ClusterConfig::default()
        };
        let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

        // (a) Adding the control plane perturbs nothing: cluster ≡ the independent oracle.
        for (i, title) in titles.iter().enumerate() {
            let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
            assert_eq!(
                got, oracle[i],
                "k={k} rf={rf}: control-plane cluster vs brute on {title:?}"
            );
        }

        // (b) The committed cluster-state document is well-formed for an in-process build.
        let st = cluster.control_state().unwrap();
        assert_eq!(
            st.epoch, 0,
            "k={k} rf={rf}: a fresh build is at control-plane epoch 0"
        );
        assert_eq!(st.num_shards as usize, k);
        assert_eq!(st.nodes.len(), 1, "one logical node in-process");
        assert_eq!(st.nodes[0].id, NodeId(0));
        assert_eq!(st.nodes[0].role, NodeRole::Manager);
        assert_eq!(st.voters, vec![NodeId(0)]);
        assert_eq!(st.assignments.len(), k, "one assignment per position");
        for (p, a) in st.assignments.iter().enumerate() {
            assert_eq!(a.position as usize, p);
            assert_eq!(a.primary, NodeId(0));
            assert!(a.replicas.is_empty());
        }

        // (c) assignment_for resolves each live position; an out-of-range one FAILS LOUD.
        for p in 0..k {
            assert_eq!(cluster.assignment_for(p).unwrap().position as usize, p);
        }
        assert!(
            cluster.assignment_for(k).is_err(),
            "k={k}: an unassigned position must error, never silently default"
        );
    }
}

/// A shard→node reassignment is answer-invariant: committing a new placement advances the
/// control-plane epoch and changes the map, but (in step 5a, where reassignment is a
/// MAP-ONLY change — no data movement yet) every title's match set is unchanged.
#[test]
fn shard_reassignment_preserves_correctness() {
    let (queries, titles) = build_corpus();
    let cfg = ClusterConfig {
        num_shards: 8,
        include_broad: true,
        ..ClusterConfig::default()
    };
    let cluster = ClusterEngine::build(vocab(), &cfg, &queries).expect("build cluster");

    let truth: Vec<HashSet<u64>> = titles
        .iter()
        .map(|t| cluster.percolate(t).unwrap().into_iter().collect())
        .collect();
    let e0 = cluster.control_state().unwrap().epoch;

    let moved = [0u32, 3, 7];
    for &p in &moved {
        cluster
            .reassign_shard(ShardAssignment {
                position: p,
                primary: NodeId(1),
                replicas: vec![NodeId(2)],
            })
            .unwrap();
    }

    let st = cluster.control_state().unwrap();
    assert!(
        st.epoch > e0,
        "each committed reassignment advances the control-plane epoch ({e0} -> {})",
        st.epoch
    );
    for &p in &moved {
        let a = cluster.assignment_for(p as usize).unwrap();
        assert_eq!(a.primary, NodeId(1), "position {p} should be reassigned");
        assert_eq!(a.replicas, vec![NodeId(2)]);
    }
    assert_eq!(
        cluster.assignment_for(1).unwrap().primary,
        NodeId(0),
        "an untouched position keeps its original assignment"
    );

    // Zero false negatives / positives across the reassignment.
    for (i, title) in titles.iter().enumerate() {
        let got: HashSet<u64> = cluster.percolate(title).unwrap().into_iter().collect();
        assert_eq!(
            got, truth[i],
            "reassignment changed a match result for {title:?}"
        );
    }
}

/// Every `ControlPlane` backend, driven by the same script, converges to the identical
/// committed document — the `NullClusterLog ≡ FileClusterLog` two-backend differential.
///
/// The openraft backend (ADR-038, step 5b) is NOT a second entry here: a faithful Raft proof
/// is inherently MULTI-node (a lone node cannot satisfy a voter-set change, and openraft commits
/// its own Blank/Membership entries, so `epoch` is not byte-comparable). Its differential lives
/// in `cluster_control_raft_oracle.rs` (`--features distributed`), where 3 real nodes converge to
/// the same voters/nodes/assignments/model this in-memory backend reaches.
#[test]
fn control_plane_backends_agree() {
    let backends: Vec<(&str, Box<dyn ControlPlane>)> = vec![(
        "in-memory",
        Box::new(InMemoryControlPlane::single_node(4, 128, 0xFEED)),
    )];

    let node = |id: u64, role: NodeRole| NodeDescriptor {
        id: NodeId(id),
        addr: Some(format!("http://127.0.0.1:{}", 50050 + id)),
        role,
    };

    let mut finals: Vec<ClusterState> = Vec::with_capacity(backends.len());
    for (name, cp) in &backends {
        cp.change_membership(vec![NodeId(0), NodeId(1), NodeId(2)])
            .unwrap();
        cp.propose(ClusterStateChange::AddNode(node(1, NodeRole::Manager)))
            .unwrap();
        cp.propose(ClusterStateChange::AddNode(node(2, NodeRole::Data)))
            .unwrap();
        cp.propose(ClusterStateChange::AssignShard(ShardAssignment {
            position: 0,
            primary: NodeId(1),
            replicas: vec![NodeId(2)],
        }))
        .unwrap();
        let v = cp
            .propose(ClusterStateChange::BumpModelVersion {
                dict_fingerprint: 0xBEEF,
            })
            .unwrap();

        let st = cp.cluster_state().unwrap();
        assert_eq!(
            v,
            cp.version().unwrap(),
            "{name}: the version a propose returns is the current committed version"
        );
        assert_eq!(st.epoch, 5, "{name}: five committed transitions");
        assert_eq!(
            st.voters,
            vec![NodeId(0), NodeId(1), NodeId(2)],
            "{name}: voter set"
        );
        let ids: Vec<u64> = st.nodes.iter().map(|n| n.id.0).collect();
        assert_eq!(ids, vec![0, 1, 2], "{name}: membership (sorted, no dups)");
        assert_eq!(st.dict_fingerprint, 0xBEEF, "{name}: model fingerprint");
        assert_eq!(st.model_version, 1, "{name}: model version bumped once");
        let a0 = st.assignments.iter().find(|a| a.position == 0).unwrap();
        assert_eq!(a0.primary, NodeId(1), "{name}: position 0 reassigned");
        assert_eq!(a0.replicas, vec![NodeId(2)]);
        finals.push((*st).clone());
    }

    for w in finals.windows(2) {
        assert_eq!(
            w[0], w[1],
            "control-plane backends diverged on the committed document"
        );
    }
}
