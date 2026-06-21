//! Vocabulary management (`_vocab`, `_vocab/learn[/_and_apply]`) and runtime engine
//! settings (`_settings`). The settings handler validates a flat JSON patch against
//! the dynamically-updatable [`EngineConfig`] knobs (ADR-022) before applying it.

use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    Json,
};
use serde::{Deserialize, Serialize};

use reverse_rusty::config::EngineConfig;

use crate::dto::ApiError;
use crate::state::AppState;

// -- PUT /_vocab
#[derive(Serialize)]
struct PutVocabResponse {
    acknowledged: bool,
    /// Number of stored queries recompiled under the new normalizer so the change
    /// takes effect immediately with zero false negatives (0 if none were affected).
    recompiled: usize,
}

/// GET /_vocab — return the current vocabulary as JSON. Reads the lock-free
/// `ArcSwap` snapshot (ADR-016) rather than locking the engine, so vocab reads
/// never block behind a writer — consistent with `/_search` and the other read
/// endpoints.
pub(crate) async fn get_vocab(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let snap = state.snapshot.load();
    let vocab = snap.vocab().cloned().unwrap_or_default();
    Json(vocab)
}

/// PUT /_vocab — replace the vocabulary, then recompile every stored query
/// under the new normalizer (same lock, before the snapshot is published) so
/// the change takes effect immediately with zero false negatives.
pub(crate) async fn put_vocab(
    State(state): State<Arc<AppState>>,
    Json(vocab): Json<reverse_rusty::vocab::Vocab>,
) -> impl IntoResponse {
    let result = {
        let mut engine = state.engine.lock();
        match engine.set_vocab(vocab) {
            Ok(_) => {
                // Recompile every stored query under the new normalizer so the
                // change takes effect with zero false negatives — under the same
                // lock and BEFORE the snapshot is published, so readers never see
                // the new normalizer against not-yet-recompiled segments.
                let recompiled = engine.recompile_stale_segments();
                (
                    StatusCode::OK,
                    Json(PutVocabResponse {
                        acknowledged: true,
                        recompiled,
                    }),
                )
                    .into_response()
            }
            Err(e) => ApiError::response(StatusCode::BAD_REQUEST, "vocab_error", e.to_string())
                .into_response(),
        }
    };
    state.publish_snapshot();
    result
}

#[derive(Deserialize)]
pub(crate) struct LearnRequest {
    queries: Vec<(u64, String)>,
    #[serde(default = "default_min_count")]
    min_count: usize,
    /// Opt-in NPMI corpus phrase induction (ADR-053); off by default.
    #[serde(default)]
    corpus_phrases: bool,
    #[serde(default)]
    npmi_tau: Option<f64>,
    #[serde(default)]
    npmi_min_count: Option<usize>,
    #[serde(default)]
    npmi_iterations: Option<usize>,
    /// Opt-in: learn any-of groups as equivalences applied via expansion (ADR-054).
    #[serde(default)]
    learn_equivalences: bool,
}

pub(crate) fn default_min_count() -> usize {
    2
}

/// Build a [`CorpusLearnConfig`](reverse_rusty::vocab::CorpusLearnConfig) from the
/// shared learn-endpoint params, falling back to the engine defaults for any absent
/// NPMI knob (so `CorpusLearnConfig::default()` stays the single source of truth).
pub(crate) fn build_corpus_config(
    min_count: usize,
    corpus_phrases: bool,
    npmi_tau: Option<f64>,
    npmi_min_count: Option<usize>,
    npmi_iterations: Option<usize>,
    learn_equivalences: bool,
) -> reverse_rusty::vocab::CorpusLearnConfig {
    let d = reverse_rusty::vocab::CorpusLearnConfig::default();
    reverse_rusty::vocab::CorpusLearnConfig {
        anyof_min_count: min_count,
        corpus_phrases,
        npmi_tau: npmi_tau.unwrap_or(d.npmi_tau),
        npmi_min_count: npmi_min_count.unwrap_or(d.npmi_min_count),
        npmi_iterations: npmi_iterations.unwrap_or(d.npmi_iterations),
        learn_equivalences,
    }
}

/// POST /_vocab/learn — learn synonyms (ADR-015 any-of) and, with
/// `corpus_phrases=true`, NPMI-induced entity phrases (ADR-053) from raw query
/// text. Returns the learned vocabulary without applying it. The caller can
/// review, edit, and then PUT /_vocab to apply.
pub(crate) async fn learn_vocab(Json(req): Json<LearnRequest>) -> impl IntoResponse {
    let cfg = build_corpus_config(
        req.min_count,
        req.corpus_phrases,
        req.npmi_tau,
        req.npmi_min_count,
        req.npmi_iterations,
        req.learn_equivalences,
    );
    let vocab = reverse_rusty::vocab::learn_vocab_from_corpus(&req.queries, &cfg);
    Json(vocab)
}

#[derive(Deserialize, Default)]
pub(crate) struct LearnApplyQuery {
    /// Minimum any-of occurrences for a synonym to be learned (ES-style query param).
    #[serde(default = "default_min_count")]
    pub(crate) min_count: usize,
    /// Opt-in NPMI corpus phrase induction (ADR-053); off by default — when absent the
    /// endpoint is byte-identical to before (any-of learning only).
    #[serde(default)]
    pub(crate) corpus_phrases: bool,
    /// NPMI binding-strength threshold (defaults to the engine default).
    #[serde(default)]
    pub(crate) npmi_tau: Option<f64>,
    /// Minimum adjacent co-occurrence count for an induced phrase.
    #[serde(default)]
    pub(crate) npmi_min_count: Option<usize>,
    /// Bigram -> trigram growth passes.
    #[serde(default)]
    pub(crate) npmi_iterations: Option<usize>,
    /// Opt-in: learn any-of groups as equivalences applied via expansion (ADR-054).
    #[serde(default)]
    pub(crate) learn_equivalences: bool,
}

#[derive(Serialize)]
struct LearnApplyResponse {
    acknowledged: bool,
    /// Number of stored queries recompiled under the learned-and-applied vocabulary.
    recompiled: usize,
}

/// POST /_vocab/learn_and_apply — learn from the engine's OWN stored queries and APPLY
/// immediately (ADR-046), recompiling the index so the change takes effect with zero false
/// negatives. By default this is ADR-015 any-of synonym learning (`?min_count=N`, default 2).
/// With `?corpus_phrases=true` it ALSO runs NPMI corpus phrase induction (ADR-053) — entity
/// phrases self-derived from the live query text — tunable via `npmi_tau`, `npmi_min_count`,
/// `npmi_iterations` (all defaulting to the engine defaults). Unlike `POST /_vocab/learn`,
/// this changes the live vocabulary.
pub(crate) async fn learn_and_apply_vocab(
    State(state): State<Arc<AppState>>,
    Query(q): Query<LearnApplyQuery>,
) -> impl IntoResponse {
    let cfg = build_corpus_config(
        q.min_count,
        q.corpus_phrases,
        q.npmi_tau,
        q.npmi_min_count,
        q.npmi_iterations,
        q.learn_equivalences,
    );
    let result = {
        let mut engine = state.engine.lock();
        match engine.learn_and_apply_with(&cfg) {
            Ok(recompiled) => (
                StatusCode::OK,
                Json(LearnApplyResponse {
                    acknowledged: true,
                    recompiled,
                }),
            )
                .into_response(),
            Err(e) => ApiError::response(StatusCode::BAD_REQUEST, "vocab_error", e.to_string())
                .into_response(),
        }
    };
    state.publish_snapshot();
    result
}

// ---------------------------------------------------------------------------
// Settings management (ES-style /_settings)
// ---------------------------------------------------------------------------

#[derive(Deserialize, Default)]
pub(crate) struct SettingsQuery {
    /// When true, `GET /_settings` also returns the default settings (ES-style).
    #[serde(default)]
    include_defaults: bool,
}

#[derive(Serialize)]
struct GetSettingsResponse {
    settings: EngineConfig,
    #[serde(skip_serializing_if = "Option::is_none")]
    defaults: Option<EngineConfig>,
}

#[derive(Serialize)]
struct PutSettingsResponse {
    acknowledged: bool,
    /// Whether the change survives a restart. Currently always `false`: settings
    /// updates are in-memory only (the startup CLI flags are the durable source).
    /// Surfaced explicitly so clients aren't surprised after a restart.
    persistent: bool,
    settings: EngineConfig,
}

/// GET /_settings — return the live engine settings as JSON. Reads the lock-free
/// snapshot (ADR-016). `?include_defaults=true` also returns the defaults, like
/// Elasticsearch's `GET /_cluster/settings?include_defaults`.
pub(crate) async fn get_settings(
    State(state): State<Arc<AppState>>,
    Query(q): Query<SettingsQuery>,
) -> impl IntoResponse {
    let settings = state.snapshot.load().config().clone();
    let defaults = q.include_defaults.then(EngineConfig::default);
    Json(GetSettingsResponse { settings, defaults })
}

/// PUT /_settings — update dynamic engine settings at runtime. The body is a flat
/// JSON object of setting keys to new values, e.g. `{"max_segments": 16}`.
/// All-or-nothing: if any key is unknown, non-dynamic, the wrong type, or would
/// produce an invalid config, nothing changes and the request is rejected with an
/// ES-style reason. Changes are in-memory (not persisted across restart).
pub(crate) async fn put_settings(
    State(state): State<Arc<AppState>>,
    Json(body): Json<serde_json::Value>,
) -> Response {
    let Some(patch) = body.as_object() else {
        return ApiError::response(
            StatusCode::BAD_REQUEST,
            "settings_error",
            "request body must be a JSON object of settings",
        )
        .into_response();
    };
    if patch.is_empty() {
        return ApiError::response(
            StatusCode::BAD_REQUEST,
            "settings_error",
            "no settings provided",
        )
        .into_response();
    }

    let updated = {
        let mut engine = state.engine.lock();
        match apply_settings_patch(engine.config().clone(), patch) {
            Ok(cfg) => {
                engine.set_config(cfg.clone());
                cfg
            }
            Err(problems) => {
                return ApiError::response(
                    StatusCode::BAD_REQUEST,
                    "settings_error",
                    problems.join("; "),
                )
                .into_response();
            }
        }
    };
    // Republish so the lock-free snapshot (and GET /_settings) reflects the change.
    state.publish_snapshot();

    Json(PutSettingsResponse {
        acknowledged: true,
        persistent: false,
        settings: updated,
    })
    .into_response()
}

/// Apply a flat settings patch to `cfg`, enforcing the dynamic/static split, key
/// validity, value types, and the engine's own `validate()` ranges. Returns the
/// updated config, or every problem found (all keys are checked, so the caller
/// sees all errors at once — and on any error nothing is applied). Pure and
/// side-effect-free, so it is unit-tested directly without the HTTP layer.
fn apply_settings_patch(
    mut cfg: EngineConfig,
    patch: &serde_json::Map<String, serde_json::Value>,
) -> Result<EngineConfig, Vec<String>> {
    let mut errors = Vec::new();
    for (key, val) in patch {
        match key.as_str() {
            // ---- dynamic knobs (runtime-tunable) ----
            "max_segments" => set_usize(&mut cfg.max_segments, key, val, &mut errors),
            "memtable_flush_threshold" => {
                set_usize(&mut cfg.memtable_flush_threshold, key, val, &mut errors);
            }
            "max_query_length" => set_usize(&mut cfg.max_query_length, key, val, &mut errors),
            "max_query_clauses" => set_usize(&mut cfg.max_query_clauses, key, val, &mut errors),
            "max_anyof_group_size" => {
                set_usize(&mut cfg.max_anyof_group_size, key, val, &mut errors);
            }
            "max_tags" => set_usize(&mut cfg.max_tags, key, val, &mut errors),
            "holes_ratio_threshold" => {
                set_f64(&mut cfg.holes_ratio_threshold, key, val, &mut errors);
            }
            "compaction_fixed_cost" => {
                set_f64(&mut cfg.compaction_fixed_cost, key, val, &mut errors);
            }
            "auto_compact_on_flush" => {
                set_bool(&mut cfg.auto_compact_on_flush, key, val, &mut errors);
            }
            "auto_compact_on_ingest" => {
                set_bool(&mut cfg.auto_compact_on_ingest, key, val, &mut errors);
            }
            "compaction_reanchor" => {
                set_bool(&mut cfg.compaction_reanchor, key, val, &mut errors);
            }
            // ---- broad-lane batch knobs (ADR-026) ----
            "broad_batch_size" => set_usize(&mut cfg.broad_batch_size, key, val, &mut errors),
            "max_percolate_batch" => {
                set_usize(&mut cfg.max_percolate_batch, key, val, &mut errors);
            }
            "broad_columnar" => set_bool(&mut cfg.broad_columnar, key, val, &mut errors),
            "broad_materialize" => set_bool(&mut cfg.broad_materialize, key, val, &mut errors),
            // ---- the class-D always-candidate lane (ADR-068) ----
            "accept_class_d" => set_bool(&mut cfg.accept_class_d, key, val, &mut errors),
            // ---- static (bound at construction) ----
            "data_dir" | "wal_sync_on_write" | "retain_source" => errors.push(format!(
                "setting [{key}] is not dynamically updateable; set it at startup"
            )),
            // ---- unknown ----
            _ => errors.push(format!("unknown setting [{key}]")),
        }
    }
    if !errors.is_empty() {
        return Err(errors);
    }
    // Range/sanity checks from the engine itself (thresholds > 0, ratio in [0,1], …).
    let problems = cfg.validate();
    if problems.is_empty() {
        Ok(cfg)
    } else {
        Err(problems)
    }
}

fn set_usize(slot: &mut usize, key: &str, val: &serde_json::Value, errors: &mut Vec<String>) {
    match val.as_u64() {
        Some(n) => *slot = n as usize,
        None => errors.push(format!("setting [{key}] must be a non-negative integer")),
    }
}

fn set_f64(slot: &mut f64, key: &str, val: &serde_json::Value, errors: &mut Vec<String>) {
    match val.as_f64() {
        Some(n) => *slot = n,
        None => errors.push(format!("setting [{key}] must be a number")),
    }
}

fn set_bool(slot: &mut bool, key: &str, val: &serde_json::Value, errors: &mut Vec<String>) {
    match val.as_bool() {
        Some(b) => *slot = b,
        None => errors.push(format!("setting [{key}] must be a boolean")),
    }
}

#[cfg(test)]
mod settings_tests {
    use super::{apply_settings_patch, EngineConfig};

    fn patch(json: &str) -> serde_json::Map<String, serde_json::Value> {
        serde_json::from_str(json).expect("test patch must be a JSON object")
    }

    #[test]
    fn applies_dynamic_settings() {
        let cfg = apply_settings_patch(
            EngineConfig::default(),
            &patch(
                r#"{"max_segments": 16, "auto_compact_on_flush": false, "holes_ratio_threshold": 0.5}"#,
            ),
        )
        .expect("valid dynamic patch");
        assert_eq!(cfg.max_segments, 16);
        assert!(!cfg.auto_compact_on_flush);
        assert!((cfg.holes_ratio_threshold - 0.5).abs() < f64::EPSILON);
        // Untouched fields keep their defaults.
        assert_eq!(cfg.memtable_flush_threshold, 100_000);
    }

    #[test]
    fn applies_broad_lane_settings() {
        let cfg = apply_settings_patch(
            EngineConfig::default(),
            &patch(
                r#"{"broad_batch_size": 512, "broad_columnar": false, "broad_materialize": false, "max_percolate_batch": 50000}"#,
            ),
        )
        .expect("valid broad-lane patch");
        assert_eq!(cfg.broad_batch_size, 512);
        assert!(!cfg.broad_columnar);
        assert!(!cfg.broad_materialize);
        assert_eq!(cfg.max_percolate_batch, 50_000);
    }

    #[test]
    fn applies_compaction_reanchor_setting() {
        // ADR-056: the re-anchor knob is dynamic (toggleable like the other compaction knobs).
        let cfg = apply_settings_patch(
            EngineConfig::default(),
            &patch(r#"{"compaction_reanchor": true}"#),
        )
        .expect("valid compaction_reanchor patch");
        assert!(cfg.compaction_reanchor);
    }

    #[test]
    fn rejects_zero_broad_batch_size() {
        let err = apply_settings_patch(
            EngineConfig::default(),
            &patch(r#"{"broad_batch_size": 0}"#),
        )
        .expect_err("broad_batch_size 0 must be rejected by validate()");
        assert!(
            err.iter()
                .any(|e| e.contains("broad_batch_size must be >= 1")),
            "{err:?}"
        );
    }

    #[test]
    fn rejects_static_settings() {
        let err = apply_settings_patch(
            EngineConfig::default(),
            &patch(r#"{"wal_sync_on_write": true}"#),
        )
        .expect_err("static setting must be rejected");
        assert!(
            err.iter().any(
                |e| e.contains("wal_sync_on_write") && e.contains("not dynamically updateable")
            ),
            "{err:?}"
        );
    }

    #[test]
    fn rejects_unknown_settings() {
        let err = apply_settings_patch(EngineConfig::default(), &patch(r#"{"bogus": 1}"#))
            .expect_err("unknown setting must be rejected");
        assert!(
            err.iter().any(|e| e.contains("unknown setting [bogus]")),
            "{err:?}"
        );
    }

    #[test]
    fn rejects_wrong_value_type() {
        let err = apply_settings_patch(
            EngineConfig::default(),
            &patch(r#"{"max_segments": "lots"}"#),
        )
        .expect_err("wrong type must be rejected");
        assert!(
            err.iter().any(|e| e.contains("non-negative integer")),
            "{err:?}"
        );
    }

    #[test]
    fn rejects_out_of_range_via_validate() {
        // 0 segments and a ratio > 1 are caught by EngineConfig::validate().
        let err = apply_settings_patch(
            EngineConfig::default(),
            &patch(r#"{"max_segments": 0, "holes_ratio_threshold": 2.0}"#),
        )
        .expect_err("invalid ranges must be rejected");
        assert!(err.iter().any(|e| e.contains("max_segments")), "{err:?}");
        assert!(
            err.iter().any(|e| e.contains("holes_ratio_threshold")),
            "{err:?}"
        );
    }

    #[test]
    fn one_bad_key_rejects_the_whole_patch() {
        // A valid key alongside a static one → the whole patch is rejected, so the
        // caller (the handler) leaves the engine config untouched.
        let err = apply_settings_patch(
            EngineConfig::default(),
            &patch(r#"{"max_segments": 12, "data_dir": "/tmp/x"}"#),
        )
        .expect_err("a static key alongside a valid one rejects the batch");
        assert!(err.iter().any(|e| e.contains("data_dir")), "{err:?}");
    }
}
