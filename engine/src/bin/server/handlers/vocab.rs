//! Vocabulary management (`_vocab`, `_vocab/learn[/_and_apply]`).

mod learn;
mod learn_apply;
mod read;
mod write;
pub(crate) use learn::{
    execute_vocab_learn, learn_vocab, vocab_learn_method_not_allowed, VocabLearnTransport,
    VOCAB_LEARN_BODY_LIMIT,
};
pub(crate) use learn_apply::{
    acquire_vocab_learn_apply_permit, finish_vocab_learn_apply_response, learn_and_apply_vocab,
    vocab_learn_apply_error_response, vocab_learn_apply_method_not_allowed,
    vocab_learn_apply_success, VocabLearnApplyTransport, VOCAB_LEARN_APPLY_BODY_LIMIT,
};
pub(crate) use read::{
    acquire_vocab_read_permit, finish_vocab_worker, get_vocab, serialize_vocab,
    vocab_method_not_allowed, VocabReadTransport, VOCAB_READ_BODY_LIMIT,
};
pub(crate) use write::{
    acquire_vocab_write_permit, finish_vocab_write_response, put_vocab, vocab_write_error_response,
    vocab_write_success, VocabWriteTransport, VOCAB_WRITE_BODY_LIMIT,
};

#[cfg(test)]
mod learn_apply_tests;
#[cfg(test)]
mod learn_tests;
#[cfg(test)]
mod write_tests;

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

#[cfg(test)]
mod read_tests;
