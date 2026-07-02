//! Learned-alias governance — the [`AliasRegistry`] (ADR-060, Phase 1).
//!
//! A first-class registry of equivalence-alias *candidates* with provenance, a
//! structural [`AliasKind`], a [`confidence`](AliasEntry::confidence) score, and a
//! lifecycle [`status`](AliasStatus). It is the **governance layer** over the ADR-054
//! equivalence-expansion mechanism: only entries the registry marks
//! [`Active`](AliasStatus::Active) contribute equivalence groups to the matcher (via
//! [`Vocab::effective_equivalence_groups`](crate::vocab::Vocab::effective_equivalence_groups));
//! candidates are recorded for review and never affect matching.
//!
//! Phase 1 is **single-token only** + **no matcher change**: a single-token spelling /
//! abbreviation variant auto-activates (FN-safe expansion), while multi-word groups (a
//! token-graph problem deferred to Phase 2), learned multi-form category alternatives
//! (`(psa, bgs, sgc)`), and mixed-`FeatureKind` groups are recorded as candidates,
//! **never silently active**.
//!
//! Admin/build-time only — never on the match hot path. Serialized inside [`Vocab`], so
//! the registry survives reopen and rides `PUT /_vocab` for free.

use serde::{Deserialize, Serialize};

use crate::dict::Dict;
use crate::normalize::Normalizer;

mod classify;
mod feedback;
mod solr;

#[cfg(test)]
mod tests;

pub use classify::AliasKind;
pub use feedback::{AliasFeedback, FeedbackEvidence, PairFeedback};

/// Where an alias group came from — drives how aggressively it auto-activates.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AliasProvenance {
    /// Imported from an operator-supplied Solr/Lucene synonym file (or declared explicitly).
    /// Operator intent ⇒ a single-token group is trusted even when its forms are distinct.
    DeclaredFile,
    /// Inferred from query any-of co-occurrence (ADR-015 learner). The least-trusted source:
    /// an any-of is a *disjunction*, not an equivalence assertion, so only clear single-token
    /// variants auto-activate; everything else is a candidate.
    LearnedFromQueries,
    /// Added directly by an operator through the API. Trusted like a declared file.
    Manual,
    /// Discovered by context-distributional similarity over the stored queries (ADR-102).
    /// Structurally noisy — the signal cannot tell substitutes from co-hyponyms — so entries
    /// from this source NEVER auto-activate, not even variant-looking pairs: review-first,
    /// always (`default_status_for` maps every kind to `Candidate`).
    LearnedDistributional,
}

/// Lifecycle status governing whether an alias affects matching.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AliasStatus {
    /// Recorded for review only; does **not** affect matching.
    Candidate,
    /// Active: contributes an equivalence group to the matcher (FN-safe expansion, ADR-054).
    Active,
    /// Reviewed and rejected: never affects matching, and kept so a later learn pass does not
    /// silently re-propose it.
    Rejected,
}

/// One governed alias group.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AliasEntry {
    /// The surface forms treated as the same entity (raw — resolved through the normalizer
    /// when applied). Sorted + deduped on insert, so it is a canonical key for the group.
    pub forms: Vec<String>,
    pub provenance: AliasProvenance,
    pub kind: AliasKind,
    pub status: AliasStatus,
    /// Review-prioritization score in `[0, 1]` (declared / manual = 1.0; a learned group
    /// scales with how many any-of groups reinforced it). Metadata only — never a
    /// correctness input.
    pub confidence: f64,
    /// Behavioral evidence from the match-feedback loop (ADR-103), stamped by
    /// `validate_and_apply`. Review metadata only — never a correctness input. Absent on old
    /// vocab JSON and on entries never validated.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub feedback: Option<FeedbackEvidence>,
}

impl AliasEntry {
    /// True if this entry is currently contributing an equivalence group to the matcher — an
    /// `Active` single-token **or multi-word** kind (multi-word is expressible since the Phase-2
    /// matcher, ADR-061). `MixedKind` still never reaches the matcher; the kind guard makes that
    /// structural (not just policy).
    #[must_use]
    pub fn is_active_for_matching(&self) -> bool {
        self.status == AliasStatus::Active
            && matches!(
                self.kind,
                AliasKind::SingleTokenVariant
                    | AliasKind::SingleTokenDistinct
                    | AliasKind::MultiWord
            )
    }
}

/// Count of entries by lifecycle status — surfaced for metrics / review (ADR-060 item 9).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct AliasSummary {
    pub active: usize,
    pub candidate: usize,
    pub rejected: usize,
}

/// A governed set of equivalence-alias groups (ADR-060). Default-empty ⇒ a no-op ⇒ the
/// vocabulary behaves exactly as before this registry existed.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct AliasRegistry {
    #[serde(default)]
    entries: Vec<AliasEntry>,
}

impl AliasRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    #[must_use]
    pub fn entries(&self) -> &[AliasEntry] {
        &self.entries
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// The forms of every entry that is currently active for matching, as raw equivalence
    /// groups. Consumed by [`Vocab::effective_equivalence_groups`](crate::vocab::Vocab::effective_equivalence_groups).
    #[must_use]
    pub fn active_groups(&self) -> Vec<Vec<String>> {
        self.entries
            .iter()
            .filter(|e| e.is_active_for_matching())
            .map(|e| e.forms.clone())
            .collect()
    }

    /// The raw forms of every entry active for matching (ADR-061), deduped + sorted. Offered to
    /// `Vocab::to_normalizer` → `NormalizerBuilder::add_alias_form`, which tokenizes each form
    /// against the **final** punctuation table and registers only those cleaning to ≥2 tokens as
    /// alias phrases (single-token forms are the equivalence map's job and register nothing).
    ///
    /// Deliberately kind-INDEPENDENT beyond the matchable-kind gate in `is_active_for_matching`:
    /// the stored `kind` is a classification *snapshot*, and a later punctuation-table change can
    /// turn a single-token form multi-word (`a-b` under `-`:Fold → `-`:Split). Trusting the
    /// snapshot would leave the still-Active alias unregistered — it would resolve to several
    /// features, be dropped from the equivalence map, and silently stop matching (codex R11).
    /// Letting the builder re-derive multi-wordness from the live table makes registration
    /// self-healing in both directions; `MixedKind` stays excluded (structurally never matchable).
    #[must_use]
    pub fn active_alias_forms(&self) -> Vec<String> {
        let mut out: Vec<String> = self
            .entries
            .iter()
            .filter(|e| e.is_active_for_matching())
            .flat_map(|e| e.forms.iter().cloned())
            .collect();
        out.sort();
        out.dedup();
        out
    }

    /// Entries awaiting review (status `Candidate`).
    pub fn candidates(&self) -> impl Iterator<Item = &AliasEntry> {
        self.entries
            .iter()
            .filter(|e| e.status == AliasStatus::Candidate)
    }

    /// Entries currently active.
    pub fn active(&self) -> impl Iterator<Item = &AliasEntry> {
        self.entries
            .iter()
            .filter(|e| e.status == AliasStatus::Active)
    }

    /// Count entries by status (ADR-060 item 9).
    #[must_use]
    pub fn summary(&self) -> AliasSummary {
        let mut s = AliasSummary::default();
        for e in &self.entries {
            match e.status {
                AliasStatus::Active => s.active += 1,
                AliasStatus::Candidate => s.candidate += 1,
                AliasStatus::Rejected => s.rejected += 1,
            }
        }
        s
    }

    /// Canonicalize raw forms: trim, drop empties, dedup, sort. Returns `None` if fewer
    /// than two distinct forms remain (an equivalence needs ≥2).
    fn canonical_forms(forms: &[String]) -> Option<Vec<String>> {
        let mut f: Vec<String> = forms.iter().map(|s| s.trim().to_string()).collect();
        f.retain(|s| !s.is_empty());
        f.sort();
        f.dedup();
        (f.len() >= 2).then_some(f)
    }

    /// Find an entry by its canonical forms key.
    fn position(&self, forms: &[String]) -> Option<usize> {
        self.entries.iter().position(|e| e.forms == forms)
    }

    /// True if a group with these (raw, to-be-canonicalized) forms already exists AND is active —
    /// so the learn/import paths can count only *newly*-active groups (a re-import reports 0).
    fn is_active_group(&self, forms: &[String]) -> bool {
        Self::canonical_forms(forms)
            .and_then(|f| self.position(&f))
            .is_some_and(|i| self.entries[i].status == AliasStatus::Active)
    }

    /// True if a group with these (raw, to-be-canonicalized) forms is already recorded, in any
    /// status — the discovery paths use it to report new-vs-rediscovered (ADR-102).
    pub fn contains(&self, forms: &[String]) -> bool {
        Self::canonical_forms(forms).is_some_and(|f| self.position(&f).is_some())
    }

    /// Add (or reconcile) a group, classifying its [`AliasKind`] against `norm`/`dict` and
    /// assigning a default [`status`](AliasStatus) from the kind + provenance policy. Returns
    /// the resulting status, or `None` if the group was rejected for having < 2 distinct forms.
    ///
    /// Reconciliation when the same forms already exist: a `Rejected` entry is left rejected
    /// (a re-learn must not resurrect it); otherwise a higher-trust provenance
    /// (declared/manual over learned) re-classifies + may promote, and confidence takes the
    /// max — so importing a declared file over a learned candidate upgrades it deterministically.
    /// A **same-provenance** re-import re-classifies and adopts a now-active default (so a
    /// persisted Phase-1 multi-word candidate activates when its synonym file is re-imported under
    /// the Phase-2 policy) but never *downgrades* an existing status — a re-learn cannot undo a
    /// manual activation (codex R7).
    pub fn add_classified(
        &mut self,
        forms: &[String],
        provenance: AliasProvenance,
        confidence: f64,
        norm: &Normalizer,
        dict: &Dict,
    ) -> Option<AliasStatus> {
        let forms = Self::canonical_forms(forms)?;
        let kind = classify::classify_kind(&forms, norm, dict);
        let status = classify::default_status_for(kind, provenance);

        if let Some(i) = self.position(&forms) {
            let existing = &mut self.entries[i];
            existing.confidence = existing.confidence.max(confidence);
            if existing.status == AliasStatus::Rejected {
                return Some(AliasStatus::Rejected);
            }
            // A more authoritative source re-decides kind/status (declared/manual win over learned).
            if provenance_rank(provenance) > provenance_rank(existing.provenance) {
                existing.provenance = provenance;
                existing.kind = kind;
                existing.status = status;
            } else if provenance_rank(provenance) == provenance_rank(existing.provenance)
                && status == AliasStatus::Active
            {
                // Same-provenance re-import: ADOPT a now-active default so a persisted candidate the
                // current policy can express becomes active (codex R7). Only ever UPGRADE the
                // status — never a downgrade: a re-import/re-learn must not undo a manual activation.
                // When PROMOTING a candidate (it was NOT already active), adopt the fresh `kind` too:
                // otherwise `is_active_for_matching` keeps seeing the stale classification (e.g. a
                // `MixedKind` candidate the current policy can now express) and the alias reports
                // active while installing no equivalence or phrase. For an ALREADY-active entry,
                // preserve the `kind` — re-classifying it to a non-matchable `kind` would silently
                // drop it from `active_groups` (codex R9).
                if existing.status != AliasStatus::Active {
                    existing.kind = kind;
                }
                existing.status = AliasStatus::Active;
            }
            return Some(existing.status);
        }

        self.entries.push(AliasEntry {
            forms,
            provenance,
            kind,
            status,
            confidence,
            feedback: None,
        });
        Some(status)
    }

    /// Learn alias candidates from query any-of groups (ADR-060 item 2). Each positive any-of
    /// group seen in ≥ `min_count` queries is classified and added with
    /// [`LearnedFromQueries`](AliasProvenance::LearnedFromQueries) provenance — so only clear
    /// single-token *variants* auto-activate; multi-word, multi-form category alternatives,
    /// and mixed-kind groups land as candidates. Returns the number of newly-active groups.
    pub fn learn_from_queries(
        &mut self,
        queries: &[(u64, String)],
        min_count: usize,
        norm: &Normalizer,
        dict: &Dict,
    ) -> usize {
        let mut activated = 0;
        for (forms, count) in super::learn_anyof_groups(queries, min_count) {
            let confidence = learned_confidence(count, min_count);
            let was_active = self.is_active_group(&forms);
            if self.add_classified(
                &forms,
                AliasProvenance::LearnedFromQueries,
                confidence,
                norm,
                dict,
            ) == Some(AliasStatus::Active)
                && !was_active
            {
                activated += 1;
            }
        }
        activated
    }

    /// Import a Solr/Lucene synonym file (ADR-060 item 3) into the registry as
    /// [`DeclaredFile`](AliasProvenance::DeclaredFile) groups. Operator intent ⇒ single-token
    /// groups activate; multi-word groups are still recorded as candidates (Phase 2 can't
    /// express them). Returns the number of newly-active groups.
    pub fn import_solr(&mut self, text: &str, norm: &Normalizer, dict: &Dict) -> usize {
        let mut activated = 0;
        for forms in solr::parse_solr_synonyms(text) {
            let was_active = self.is_active_group(&forms);
            if self.add_classified(&forms, AliasProvenance::DeclaredFile, 1.0, norm, dict)
                == Some(AliasStatus::Active)
                && !was_active
            {
                activated += 1;
            }
        }
        activated
    }

    /// Promote a candidate to [`Active`](AliasStatus::Active). Refuses (returns `false`) a
    /// `MixedKind` group — the one kind the matcher still cannot express safely — so review can
    /// never activate something it would silently ignore. Multi-word groups are now accepted
    /// (the Phase-2 matcher expresses them, ADR-061). `forms` are canonicalized before lookup.
    pub fn activate(&mut self, forms: &[String]) -> bool {
        let Some(forms) = Self::canonical_forms(forms) else {
            return false;
        };
        let Some(i) = self.position(&forms) else {
            return false;
        };
        let e = &mut self.entries[i];
        if e.kind == AliasKind::MixedKind {
            return false;
        }
        e.status = AliasStatus::Active;
        true
    }

    /// Mark a group [`Rejected`](AliasStatus::Rejected) so it no longer matches and a later
    /// learn pass will not silently re-propose it. `forms` are canonicalized before lookup.
    pub fn reject(&mut self, forms: &[String]) -> bool {
        let Some(forms) = Self::canonical_forms(forms) else {
            return false;
        };
        let Some(i) = self.position(&forms) else {
            return false;
        };
        self.entries[i].status = AliasStatus::Rejected;
        true
    }

    /// Stamp match-feedback evidence onto an entry (ADR-103): sets
    /// [`feedback`](AliasEntry::feedback) and raises `confidence` to at least the measured
    /// overlap (finite-guarded; reconcile-by-max, like re-learning). Pure metadata — status is
    /// untouched. Returns `false` for an unknown group.
    pub fn record_feedback(&mut self, forms: &[String], evidence: FeedbackEvidence) -> bool {
        let Some(forms) = Self::canonical_forms(forms) else {
            return false;
        };
        let Some(i) = self.position(&forms) else {
            return false;
        };
        let e = &mut self.entries[i];
        if evidence.overlap.is_finite() {
            e.confidence = e.confidence.max(evidence.overlap.clamp(0.0, 1.0));
        }
        e.feedback = Some(evidence);
        true
    }

    /// The **automated** activation used by feedback validation (`validate_and_apply?activate=true`,
    /// ADR-103): promotes a `Candidate` to `Active`. Unlike the operator-override
    /// [`activate`](Self::activate) it acts ONLY on a `Candidate` — a `Rejected` entry is
    /// refused (an automated pass must never resurrect an operator's rejection), `MixedKind`
    /// is refused (structurally unexpressible), and an already-`Active` entry returns `false`
    /// so a racing/repeated validate pass is idempotent and never triggers a spurious
    /// full-recompile apply (codex review).
    pub fn activate_validated(&mut self, forms: &[String]) -> bool {
        let Some(forms) = Self::canonical_forms(forms) else {
            return false;
        };
        let Some(i) = self.position(&forms) else {
            return false;
        };
        let e = &mut self.entries[i];
        if e.status != AliasStatus::Candidate || e.kind == AliasKind::MixedKind {
            return false;
        }
        e.status = AliasStatus::Active;
        true
    }

    /// Demote every ACTIVE entry containing a form the CURRENT normalizer cannot express — a
    /// form that cleans to fewer than two tokens AND does not resolve to exactly one feature
    /// (e.g. a fused grader `psa10` after a punctuation refold turned `psa-10` into one token) —
    /// back to [`Candidate`](AliasStatus::Candidate) (codex R13). Such a form cannot be
    /// registered as an alias phrase and `resolve_equivalences` drops it, so leaving the entry
    /// Active would report an alias that silently never matches. Called by every
    /// equivalence-install seam (engine/cluster `set_vocab`, `adopt_vocab`, `with_vocab`,
    /// `open_with_vocab`), so Active always reflects what the live normalizer expresses.
    ///
    /// The demotion is **status-only** — the stored `kind` is preserved (codex R14): stamping
    /// `MixedKind` would dead-end the entry, since [`activate`](Self::activate) structurally
    /// refuses that kind even after the operator repairs the vocabulary. With the kind intact, a
    /// repaired configuration re-activates via `activate` or a re-import (whose same-provenance
    /// promotion adopts the fresh kind, codex R10); an activate while still broken simply
    /// demotes again at the next install seam. Deliberately NOT a full reclassification:
    /// re-deriving the *kind* (e.g. cross-kind drift as the dict learns) would demote working
    /// aliases on a precision hunch — equivalence expansion only ever widens any-of groups
    /// (recall-safe), so kind drift after activation is accepted and corrected on re-import.
    /// Returns the demoted count.
    pub fn demote_unexpressible(&mut self, norm: &Normalizer, dict: &Dict) -> usize {
        let mut lc = String::new();
        let mut demoted = 0;
        for e in &mut self.entries {
            if !e.is_active_for_matching() {
                continue;
            }
            let unexpressible = e.forms.iter().any(|f| {
                norm.clean_tokens(f).len() < 2
                    && norm.compile_features_readonly(f, dict, &mut lc).len() != 1
            });
            if unexpressible {
                e.status = AliasStatus::Candidate;
                demoted += 1;
            }
        }
        demoted
    }

    /// Merge another registry into this one (used by [`Vocab::merge`](crate::vocab::Vocab::merge)).
    /// An incoming entry whose forms are new is appended verbatim; a clash keeps the existing
    /// entry (first wins) but adopts a higher-trust provenance + the max confidence, mirroring
    /// [`add_classified`](Self::add_classified)'s reconciliation without needing a normalizer.
    pub fn merge(&mut self, other: &AliasRegistry) {
        for incoming in &other.entries {
            if let Some(i) = self.position(&incoming.forms) {
                let existing = &mut self.entries[i];
                existing.confidence = existing.confidence.max(incoming.confidence);
                if existing.status != AliasStatus::Rejected
                    && provenance_rank(incoming.provenance) > provenance_rank(existing.provenance)
                {
                    existing.provenance = incoming.provenance;
                    existing.kind = incoming.kind;
                    existing.status = incoming.status;
                }
            } else {
                self.entries.push(incoming.clone());
            }
        }
    }
}

/// Trust ordering for reconciliation: declared/manual outrank a learned guess. Both learned
/// sources share the bottom rank — reconciliation safety for the distributional source then
/// follows structurally: the same-rank promotion branch in [`AliasRegistry::add_classified`]
/// requires the *computed* status to be `Active`, which `LearnedDistributional` never produces,
/// so a re-discovery can only max confidence (and an any-of re-learn may still promote a
/// variant under ITS OWN policy — that is ADR-060's trust level acting on its own signal).
fn provenance_rank(p: AliasProvenance) -> u8 {
    match p {
        AliasProvenance::LearnedFromQueries | AliasProvenance::LearnedDistributional => 0,
        AliasProvenance::DeclaredFile | AliasProvenance::Manual => 1,
    }
}

/// Map a learned group's reinforcement count to a `[0, 1)` confidence: a group seen exactly
/// `min_count` times scores 0.5, approaching 1 as it recurs. Pure metadata for review sort
/// order — never a correctness input.
fn learned_confidence(count: usize, min_count: usize) -> f64 {
    let c = count as f64;
    let m = (min_count.max(1)) as f64;
    c / (c + m)
}
