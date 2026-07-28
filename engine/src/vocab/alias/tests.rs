//! Unit tests for the alias registry, its structural classifier, and the Solr import.

use super::classify::{classify_kind, default_status_for, AliasKind};
use super::solr::parse_solr_synonyms;
use super::{AliasProvenance, AliasRegistry, AliasStatus};
use crate::dict::Dict;
use crate::normalize::Normalizer;

fn norm() -> Normalizer {
    Normalizer::default_vocab().expect("default normalizer")
}

fn forms(fs: &[&str]) -> Vec<String> {
    fs.iter().map(|s| (*s).to_string()).collect()
}

// ── Classifier ───────────────────────────────────────────────────────────────

mod classification;
mod imports;
mod persistence;
mod solr;
