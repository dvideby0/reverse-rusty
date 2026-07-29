//! Versioned CPU ranking profiles (ADR-162).
//!
//! Profiles are serving configuration, not query-corpus state. The built-in
//! `static_v1` profile preserves the historical priority + tag-boost score.
//! Operator-loaded linear and tree-ensemble profiles add a deterministic
//! relevance term after Boolean matching. Every feature and operation is
//! integer-only, and model shape is validated and bounded before admission.

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::sync::Arc;

use serde::de::{MapAccess, Visitor};
use serde::{Deserialize, Deserializer, Serialize};

use crate::util::{fast_map, FastMap};

mod tree;

use tree::{CompiledTree, TreeConfig, TreeNodeConfig};

pub const STATIC_RANK_PROFILE: &str = "static_v1";

const PROFILE_SCHEMA_VERSION: u32 = 1;
const MAX_PROFILE_FILE_BYTES: usize = 16 * 1024 * 1024;
const MAX_PROFILES: usize = 64;
const MAX_LINEAR_TERMS: usize = 64;
const MAX_TREES: usize = 256;
const MAX_TREE_NODES: usize = 16_384;
const MAX_TREE_DEPTH: usize = 16;
const MAX_TREE_EVAL_STEPS: usize = 1_024;

/// Stable feature schema consumed by CPU ranking profiles.
///
/// New semantics require a new feature name; existing names are immutable so a
/// model file cannot silently change meaning across binaries.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RankFeature {
    QueryPositiveTerms,
    QueryNegativeTerms,
    QueryAnyOfGroups,
    QueryTagCount,
    TitleTokens,
    TitleBytes,
    TitleDigits,
    PositiveCoverageMilli,
    UnmatchedTitleTokens,
}

/// Query-side evidence derived from the already-persisted exact-verification
/// columns. It adds no durable format and is read only after Boolean truth.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RankQueryFeatures {
    pub positive_terms: u32,
    pub negative_terms: u32,
    pub any_of_groups: u32,
    pub tag_count: u32,
}

/// Allocation-free title evidence shared by every matched query for one title.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct RankTitleFeatures {
    pub tokens: u32,
    pub bytes: u32,
    pub digits: u32,
}

impl RankTitleFeatures {
    #[must_use]
    pub fn from_title(title: &str) -> Self {
        let mut tokens = 0u32;
        let mut in_token = false;
        let mut digits = 0u32;
        for ch in title.chars() {
            let alphanumeric = ch.is_alphanumeric();
            if alphanumeric && !in_token {
                tokens = tokens.saturating_add(1);
            }
            if ch.is_numeric() {
                digits = digits.saturating_add(1);
            }
            in_token = alphanumeric;
        }
        Self {
            tokens,
            bytes: u32::try_from(title.len()).unwrap_or(u32::MAX),
            digits,
        }
    }
}

#[derive(Clone, Copy)]
pub(crate) struct RankFeatureView {
    query: RankQueryFeatures,
    title: RankTitleFeatures,
}

impl RankFeatureView {
    pub(crate) fn new(query: RankQueryFeatures, title: RankTitleFeatures) -> Self {
        Self { query, title }
    }

    #[inline]
    fn value(self, feature: RankFeature) -> i64 {
        match feature {
            RankFeature::QueryPositiveTerms => i64::from(self.query.positive_terms),
            RankFeature::QueryNegativeTerms => i64::from(self.query.negative_terms),
            RankFeature::QueryAnyOfGroups => i64::from(self.query.any_of_groups),
            RankFeature::QueryTagCount => i64::from(self.query.tag_count),
            RankFeature::TitleTokens => i64::from(self.title.tokens),
            RankFeature::TitleBytes => i64::from(self.title.bytes),
            RankFeature::TitleDigits => i64::from(self.title.digits),
            RankFeature::PositiveCoverageMilli => {
                let denominator = self.title.tokens.max(1);
                i64::from(
                    self.query
                        .positive_terms
                        .saturating_mul(1_000)
                        .checked_div(denominator)
                        .unwrap_or(0),
                )
            }
            RankFeature::UnmatchedTitleTokens => {
                i64::from(self.title.tokens.saturating_sub(self.query.positive_terms))
            }
        }
    }
}

/// Validated collection of named ranking profiles. `static_v1` is always
/// present and cannot be redefined with different semantics.
#[derive(Clone, Debug)]
pub struct RankProfiles {
    profiles: FastMap<String, RegisteredProfile>,
}

impl Default for RankProfiles {
    fn default() -> Self {
        let mut profiles = fast_map();
        let program = Arc::new(RankProfileProgram::Static);
        profiles.insert(
            STATIC_RANK_PROFILE.to_string(),
            RegisteredProfile {
                fingerprint: program.fingerprint(),
                program,
            },
        );
        Self { profiles }
    }
}

impl RankProfiles {
    /// Parse, validate, and bound one profile configuration.
    pub fn from_json_slice(bytes: &[u8]) -> Result<Self, RankProfileError> {
        if bytes.len() > MAX_PROFILE_FILE_BYTES {
            return Err(RankProfileError::Invalid(format!(
                "ranking profile file is {} bytes; maximum is {MAX_PROFILE_FILE_BYTES}",
                bytes.len()
            )));
        }
        let config: ProfileFile = serde_json::from_slice(bytes)
            .map_err(|error| RankProfileError::Json(error.to_string()))?;
        Self::from_config(config)
    }

    /// Load one bounded JSON configuration from disk.
    pub fn load_json(path: impl AsRef<Path>) -> Result<Self, RankProfileError> {
        let metadata = std::fs::metadata(path.as_ref())
            .map_err(|error| RankProfileError::Io(error.to_string()))?;
        if metadata.len() > MAX_PROFILE_FILE_BYTES as u64 {
            return Err(RankProfileError::Invalid(format!(
                "ranking profile file is {} bytes; maximum is {MAX_PROFILE_FILE_BYTES}",
                metadata.len()
            )));
        }
        let bytes = std::fs::read(path.as_ref())
            .map_err(|error| RankProfileError::Io(error.to_string()))?;
        Self::from_json_slice(&bytes)
    }

    #[must_use]
    pub fn contains(&self, name: &str) -> bool {
        self.profiles.contains_key(name)
    }

    pub fn names(&self) -> impl Iterator<Item = &str> {
        self.profiles.keys().map(String::as_str)
    }

    /// Stable semantic fingerprint of one validated model.
    #[must_use]
    pub fn fingerprint(&self, name: &str) -> Option<u64> {
        self.profiles.get(name).map(|profile| profile.fingerprint)
    }

    pub(crate) fn get(&self, name: &str) -> Option<&RegisteredProfile> {
        self.profiles.get(name)
    }

    fn from_config(config: ProfileFile) -> Result<Self, RankProfileError> {
        if config.version != PROFILE_SCHEMA_VERSION {
            return Err(RankProfileError::Invalid(format!(
                "unsupported ranking profile schema version {}; expected {PROFILE_SCHEMA_VERSION}",
                config.version
            )));
        }
        if config.profiles.len() > MAX_PROFILES {
            return Err(RankProfileError::Invalid(format!(
                "ranking profile count {} exceeds maximum {MAX_PROFILES}",
                config.profiles.len()
            )));
        }
        let mut registry = Self::default();
        for (name, profile) in config.profiles {
            validate_profile_name(&name)?;
            let expected = profile.expected_fingerprint();
            let compiled = RankProfileProgram::compile(&name, profile)?;
            if name == STATIC_RANK_PROFILE && !matches!(compiled, RankProfileProgram::Static) {
                return Err(RankProfileError::Invalid(format!(
                    "`{STATIC_RANK_PROFILE}` is reserved for the built-in static scorer"
                )));
            }
            let fingerprint = compiled.fingerprint();
            if let Some(expected) = expected {
                let actual = format!("fnv1a64:{fingerprint:016x}");
                if expected != actual {
                    return Err(RankProfileError::Invalid(format!(
                        "profile `{name}` fingerprint mismatch: configured `{expected}`, \
                         computed `{actual}`"
                    )));
                }
            }
            registry.profiles.insert(
                name,
                RegisteredProfile {
                    program: Arc::new(compiled),
                    fingerprint,
                },
            );
        }
        Ok(registry)
    }
}

/// Startup/configuration failure for a ranking profile registry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RankProfileError {
    Io(String),
    Json(String),
    Invalid(String),
}

impl std::fmt::Display for RankProfileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io(reason) => write!(f, "cannot read ranking profiles: {reason}"),
            Self::Json(reason) => write!(f, "invalid ranking profile JSON: {reason}"),
            Self::Invalid(reason) => f.write_str(reason),
        }
    }
}

impl std::error::Error for RankProfileError {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileFile {
    version: u32,
    #[serde(deserialize_with = "deserialize_unique_profiles")]
    profiles: BTreeMap<String, ProfileConfig>,
}

fn deserialize_unique_profiles<'de, D>(
    deserializer: D,
) -> Result<BTreeMap<String, ProfileConfig>, D::Error>
where
    D: Deserializer<'de>,
{
    struct UniqueProfiles;

    impl<'de> Visitor<'de> for UniqueProfiles {
        type Value = BTreeMap<String, ProfileConfig>;

        fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            formatter.write_str("an object with unique ranking profile names")
        }

        fn visit_map<A>(self, mut entries: A) -> Result<Self::Value, A::Error>
        where
            A: MapAccess<'de>,
        {
            let mut profiles = BTreeMap::new();
            while let Some(name) = entries.next_key::<String>()? {
                if profiles.contains_key(&name) {
                    return Err(serde::de::Error::custom(format!(
                        "duplicate ranking profile name `{name}`"
                    )));
                }
                if profiles.len() == MAX_PROFILES {
                    return Err(serde::de::Error::custom(format!(
                        "ranking profile count exceeds maximum {MAX_PROFILES}"
                    )));
                }
                let profile = entries.next_value::<ProfileConfig>()?;
                profiles.insert(name, profile);
            }
            Ok(profiles)
        }
    }

    deserializer.deserialize_map(UniqueProfiles)
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ProfileConfig {
    Static {
        expected_fingerprint: Option<String>,
    },
    Linear {
        expected_fingerprint: Option<String>,
        #[serde(default)]
        intercept: i64,
        weights: Vec<LinearWeight>,
    },
    TreeEnsemble {
        expected_fingerprint: Option<String>,
        #[serde(default)]
        base_score: i64,
        trees: Vec<TreeConfig>,
    },
}

impl ProfileConfig {
    fn expected_fingerprint(&self) -> Option<String> {
        match self {
            Self::Static {
                expected_fingerprint,
            }
            | Self::Linear {
                expected_fingerprint,
                ..
            }
            | Self::TreeEnsemble {
                expected_fingerprint,
                ..
            } => expected_fingerprint.clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct LinearWeight {
    feature: RankFeature,
    weight: i64,
}

#[derive(Clone, Debug)]
pub(crate) enum RankProfileProgram {
    Static,
    Linear {
        intercept: i64,
        weights: Vec<(RankFeature, i64)>,
    },
    TreeEnsemble {
        base_score: i64,
        trees: Vec<CompiledTree>,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct RegisteredProfile {
    pub(crate) program: Arc<RankProfileProgram>,
    pub(crate) fingerprint: u64,
}

impl RankProfileProgram {
    fn compile(name: &str, profile: ProfileConfig) -> Result<Self, RankProfileError> {
        match profile {
            ProfileConfig::Static { .. } => Ok(Self::Static),
            ProfileConfig::Linear {
                intercept, weights, ..
            } => {
                if weights.len() > MAX_LINEAR_TERMS {
                    return Err(RankProfileError::Invalid(format!(
                        "profile `{name}` has {} linear weights; maximum is {MAX_LINEAR_TERMS}",
                        weights.len()
                    )));
                }
                let mut seen = BTreeSet::new();
                let mut compiled = Vec::with_capacity(weights.len());
                for term in weights {
                    if !seen.insert(term.feature) {
                        return Err(RankProfileError::Invalid(format!(
                            "profile `{name}` repeats linear feature `{:?}`",
                            term.feature
                        )));
                    }
                    compiled.push((term.feature, term.weight));
                }
                Ok(Self::Linear {
                    intercept,
                    weights: compiled,
                })
            }
            ProfileConfig::TreeEnsemble {
                base_score, trees, ..
            } => {
                if trees.is_empty() || trees.len() > MAX_TREES {
                    return Err(RankProfileError::Invalid(format!(
                        "profile `{name}` tree count {} is outside 1..={MAX_TREES}",
                        trees.len()
                    )));
                }
                let total_nodes = trees
                    .iter()
                    .map(|tree| tree.nodes.len())
                    .fold(0usize, usize::saturating_add);
                if total_nodes > MAX_TREE_NODES {
                    return Err(RankProfileError::Invalid(format!(
                        "profile `{name}` has {total_nodes} tree nodes; maximum is {MAX_TREE_NODES}"
                    )));
                }
                let trees = trees
                    .into_iter()
                    .enumerate()
                    .map(|(index, tree)| CompiledTree::compile(name, index, tree))
                    .collect::<Result<Vec<_>, _>>()?;
                let eval_steps = trees
                    .iter()
                    .map(|tree| tree.max_depth)
                    .fold(0usize, usize::saturating_add);
                if eval_steps > MAX_TREE_EVAL_STEPS {
                    return Err(RankProfileError::Invalid(format!(
                        "profile `{name}` can evaluate {eval_steps} tree nodes per match; \
                         maximum is {MAX_TREE_EVAL_STEPS}"
                    )));
                }
                Ok(Self::TreeEnsemble { base_score, trees })
            }
        }
    }

    #[inline]
    pub(crate) fn relevance_score(&self, features: RankFeatureView) -> i64 {
        match self {
            Self::Static => 0,
            Self::Linear { intercept, weights } => {
                weights.iter().fold(*intercept, |score, term| {
                    score.saturating_add(features.value(term.0).saturating_mul(term.1))
                })
            }
            Self::TreeEnsemble { base_score, trees } => {
                trees.iter().fold(*base_score, |score, tree| {
                    score.saturating_add(tree.score(features))
                })
            }
        }
    }

    pub(crate) fn is_static(&self) -> bool {
        matches!(self, Self::Static)
    }

    pub(crate) fn fingerprint(&self) -> u64 {
        let mut hash = 0xcbf2_9ce4_8422_2325u64;
        let mut add = |bytes: &[u8]| {
            for byte in bytes {
                hash ^= u64::from(*byte);
                hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
            }
        };
        match self {
            Self::Static => add(&[0]),
            Self::Linear { intercept, weights } => {
                add(&[1]);
                add(&intercept.to_le_bytes());
                add(&(weights.len() as u64).to_le_bytes());
                for (feature, weight) in weights {
                    add(&[feature.code()]);
                    add(&weight.to_le_bytes());
                }
            }
            Self::TreeEnsemble { base_score, trees } => {
                add(&[2]);
                add(&base_score.to_le_bytes());
                add(&(trees.len() as u64).to_le_bytes());
                for tree in trees {
                    add(&(tree.nodes.len() as u64).to_le_bytes());
                    for node in &tree.nodes {
                        match node {
                            TreeNodeConfig::Split {
                                feature,
                                threshold,
                                left,
                                right,
                            } => {
                                add(&[0, feature.code()]);
                                add(&threshold.to_le_bytes());
                                add(&(*left as u64).to_le_bytes());
                                add(&(*right as u64).to_le_bytes());
                            }
                            TreeNodeConfig::Leaf { value } => {
                                add(&[1]);
                                add(&value.to_le_bytes());
                            }
                        }
                    }
                }
            }
        }
        hash
    }
}

impl RankFeature {
    fn code(self) -> u8 {
        match self {
            Self::QueryPositiveTerms => 0,
            Self::QueryNegativeTerms => 1,
            Self::QueryAnyOfGroups => 2,
            Self::QueryTagCount => 3,
            Self::TitleTokens => 4,
            Self::TitleBytes => 5,
            Self::TitleDigits => 6,
            Self::PositiveCoverageMilli => 7,
            Self::UnmatchedTitleTokens => 8,
        }
    }
}

fn validate_profile_name(name: &str) -> Result<(), RankProfileError> {
    let valid = !name.is_empty()
        && name.len() <= 64
        && name.bytes().all(|byte| {
            byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"_-".contains(&byte)
        });
    if valid {
        Ok(())
    } else {
        Err(RankProfileError::Invalid(format!(
            "invalid ranking profile name `{name}`; use 1-64 lowercase ASCII letters, digits, `_` or `-`"
        )))
    }
}

#[cfg(test)]
mod tests;
