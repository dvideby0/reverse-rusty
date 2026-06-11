//! Cluster-mode vocabulary + alias + settings handlers (ADR-070). The vocabulary
//! paths map onto the cluster's own `set_vocab` machinery (ADR-046 blue/green
//! rebuild) — its one built-in refusal (non-local shards; ADR-046/076) surfaces
//! as a 400 carrying the engine's message, never weakened. A tagged cluster is
//! NOT refused (tags carry through by stored `TagId`, ADR-074), and a multi-word
//! alias activates (P(T)-aware routing, ADR-076). These are the only handlers
//! that take the cluster WRITE lock.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};
use tracing::{info, instrument};

use reverse_rusty::config::EngineConfig;
use reverse_rusty::vocab::{AliasRegistry, AliasSummary, Vocab};

use crate::handlers::vocab::{build_corpus_config, default_min_count, LearnApplyQuery};
use crate::state::ClusterAppState;

use super::{not_in_cluster_mode, shard_error_response};

/// GET /_vocab — the installed vocabulary (empty default when the cluster was
/// built directly from a normalizer).
pub(crate) async fn cluster_get_vocab(
    State(state): State<Arc<ClusterAppState>>,
) -> impl IntoResponse {
    let cluster = state.cluster.read();
    Json(cluster.vocab().cloned().unwrap_or_default())
}

#[derive(Serialize)]
struct VocabApplyResponse {
    acknowledged: bool,
    /// Live queries rebuilt under the new normalizer (the blue/green re-place).
    rebuilt: usize,
}

/// PUT /_vocab — replace the cluster vocabulary (ADR-046 mechanism 2): re-mint the
/// dict, re-place every query, atomic swap; durable clusters checkpoint the new
/// state. The non-local refusal comes back as a 400 (tags + multi-word activate, ADR-074/076).
#[instrument(skip_all)]
pub(crate) async fn cluster_put_vocab(
    State(state): State<Arc<ClusterAppState>>,
    Json(vocab): Json<Vocab>,
) -> Response {
    let result = {
        let _w = state.write_serial.lock();
        let mut cluster = state.cluster.write();
        cluster.set_vocab(vocab)
    };
    match result {
        Ok(rebuilt) => {
            info!(rebuilt, "cluster vocabulary replaced");
            Json(VocabApplyResponse {
                acknowledged: true,
                rebuilt,
            })
            .into_response()
        }
        Err(e) => shard_error_response("vocabulary change refused", &e),
    }
}

#[derive(Deserialize)]
pub(crate) struct ClusterLearnRequest {
    /// Corpus to learn from. Absent ⇒ learn from the CLUSTER's own live queries
    /// (a strict superset of the single-node shape, which requires `queries`).
    #[serde(default)]
    queries: Option<Vec<(u64, String)>>,
    #[serde(default = "default_min_count")]
    min_count: usize,
    #[serde(default)]
    corpus_phrases: bool,
    #[serde(default)]
    npmi_tau: Option<f64>,
    #[serde(default)]
    npmi_min_count: Option<usize>,
    #[serde(default)]
    npmi_iterations: Option<usize>,
    #[serde(default)]
    learn_equivalences: bool,
}

/// POST /_vocab/learn — compute-only dry run: learn vocabulary rules from the
/// supplied corpus (or, when `queries` is absent, the cluster's own live corpus)
/// and return them WITHOUT applying. Review, then `PUT /_vocab`.
#[instrument(skip_all)]
pub(crate) async fn cluster_learn_vocab(
    State(state): State<Arc<ClusterAppState>>,
    Json(req): Json<ClusterLearnRequest>,
) -> Response {
    let cfg = build_corpus_config(
        req.min_count,
        req.corpus_phrases,
        req.npmi_tau,
        req.npmi_min_count,
        req.npmi_iterations,
        req.learn_equivalences,
    );
    if let Some(queries) = req.queries {
        return Json(reverse_rusty::vocab::learn_vocab_from_corpus(
            &queries, &cfg,
        ))
        .into_response();
    }
    let result = {
        let cluster = state.cluster.read();
        cluster.learn_vocab(&cfg)
    };
    match result {
        Ok(v) => Json(v).into_response(),
        Err(e) => shard_error_response("corpus gather failed", &e),
    }
}

/// POST /_vocab/learn_and_apply — learn from the cluster's own live corpus and
/// apply via the blue/green rebuild. Same query params as single-node mode.
#[instrument(skip_all)]
pub(crate) async fn cluster_learn_and_apply_vocab(
    State(state): State<Arc<ClusterAppState>>,
    Query(q): Query<LearnApplyQuery>,
) -> Response {
    let cfg = build_corpus_config(
        q.min_count,
        q.corpus_phrases,
        q.npmi_tau,
        q.npmi_min_count,
        q.npmi_iterations,
        q.learn_equivalences,
    );
    let result = {
        let _w = state.write_serial.lock();
        let mut cluster = state.cluster.write();
        cluster.learn_and_apply_with(&cfg)
    };
    match result {
        Ok(rebuilt) => {
            info!(rebuilt, "cluster learn-and-apply complete");
            Json(VocabApplyResponse {
                acknowledged: true,
                rebuilt,
            })
            .into_response()
        }
        Err(e) => shard_error_response("learn-and-apply refused", &e),
    }
}

#[derive(Serialize)]
struct ClusterAliasesResponse {
    aliases: AliasRegistry,
    summary: AliasSummary,
}

/// GET /_vocab/aliases — the governed alias registry + status summary (ADR-060).
pub(crate) async fn cluster_get_aliases(
    State(state): State<Arc<ClusterAppState>>,
) -> impl IntoResponse {
    let cluster = state.cluster.read();
    let vocab = cluster.vocab().cloned().unwrap_or_default();
    Json(ClusterAliasesResponse {
        summary: vocab.alias_summary(),
        aliases: vocab.aliases().clone(),
    })
}

#[derive(Deserialize)]
pub(crate) struct ClusterImportAliasesRequest {
    /// Raw Solr/Lucene synonym-file text.
    synonyms: String,
}

#[derive(Serialize)]
struct ClusterAliasApplyResponse {
    acknowledged: bool,
    activated: usize,
    /// Live queries rebuilt by the apply (the cluster's `recompiled` analogue).
    rebuilt: usize,
    summary: AliasSummary,
}

/// POST /_vocab/aliases/import — import a Solr/Lucene synonym file into the
/// registry and apply via the cluster rebuild (ADR-060 at the cluster).
#[instrument(skip_all)]
pub(crate) async fn cluster_import_aliases(
    State(state): State<Arc<ClusterAppState>>,
    Json(req): Json<ClusterImportAliasesRequest>,
) -> Response {
    let result = {
        let _w = state.write_serial.lock();
        let mut cluster = state.cluster.write();
        cluster.import_alias_synonyms(&req.synonyms)
    };
    match result {
        Ok(report) => {
            info!(
                activated = report.activated,
                rebuilt = report.recompiled,
                "cluster alias import applied"
            );
            Json(ClusterAliasApplyResponse {
                acknowledged: true,
                activated: report.activated,
                rebuilt: report.recompiled,
                summary: report.summary,
            })
            .into_response()
        }
        Err(e) => shard_error_response("alias import refused", &e),
    }
}

#[derive(Deserialize, Default)]
pub(crate) struct ClusterAliasLearnQuery {
    #[serde(default = "default_min_count")]
    min_count: usize,
}

/// POST /_vocab/aliases/learn_and_apply — learn alias candidates from the
/// cluster's own stored queries and apply (conservative auto-activation, ADR-060).
#[instrument(skip_all)]
pub(crate) async fn cluster_learn_aliases(
    State(state): State<Arc<ClusterAppState>>,
    Query(q): Query<ClusterAliasLearnQuery>,
) -> Response {
    let result = {
        let _w = state.write_serial.lock();
        let mut cluster = state.cluster.write();
        cluster.learn_aliases_and_apply(q.min_count)
    };
    match result {
        Ok(report) => {
            info!(
                activated = report.activated,
                rebuilt = report.recompiled,
                "cluster alias learn-and-apply complete"
            );
            Json(ClusterAliasApplyResponse {
                acknowledged: true,
                activated: report.activated,
                rebuilt: report.recompiled,
                summary: report.summary,
            })
            .into_response()
        }
        Err(e) => shard_error_response("alias learn refused", &e),
    }
}

#[derive(Serialize)]
struct ClusterSettingsResponse {
    mode: &'static str,
    shards: usize,
    replication_factor: usize,
    include_broad: bool,
    durable: bool,
    /// The per-shard engine configuration the cluster was assembled with.
    per_shard: EngineConfig,
}

/// GET /_settings — the cluster + per-shard configuration (read-only in v1).
pub(crate) async fn cluster_get_settings(
    State(state): State<Arc<ClusterAppState>>,
) -> impl IntoResponse {
    let cluster = state.cluster.read();
    Json(ClusterSettingsResponse {
        mode: "cluster",
        shards: cluster.num_shards(),
        replication_factor: cluster.replication_factor(),
        include_broad: state.include_broad,
        durable: cluster.is_durable(),
        per_shard: cluster.per_shard_config().clone(),
    })
}

/// PUT /_settings — cluster settings are static in v1 (set at assembly); the
/// single-node dynamic-settings machinery has no cluster analogue yet.
pub(crate) async fn cluster_put_settings() -> Response {
    not_in_cluster_mode(
        "PUT /_settings",
        "cluster settings are fixed at assembly in v1 — restart the coordinator with \
         the new flags",
    )
}
