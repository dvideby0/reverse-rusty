//! Learned-alias governance endpoints (ADR-060): `/_vocab/aliases*`.
//!
//! A thin HTTP layer over the engine's alias registry: review the governed candidates, import a
//! Solr/Lucene synonym file, or learn candidates from the engine's own stored queries — each
//! reusing the engine's `set_vocab` + `recompile_stale_segments` apply path (no restart) so a safe
//! active alias takes effect immediately with zero false negatives. ADR-061 supplies the
//! multi-word matcher; declared multi-word groups may therefore activate, while learned
//! multi-word guesses remain review candidates.

mod discover;
mod discover_record;
#[cfg(test)]
mod discover_record_tests;
#[cfg(test)]
mod discover_tests;
mod feedback;
mod feedback_read;
#[cfg(test)]
mod feedback_read_tests;
mod import;
#[cfg(test)]
mod import_tests;
mod learn_apply;
#[cfg(test)]
mod learn_apply_tests;
mod read;
#[cfg(test)]
mod read_tests;
pub(crate) use discover::{
    alias_discover_method_not_allowed, discover_aliases, execute_alias_discovery,
    AliasDiscoverTransport, ALIAS_DISCOVER_BODY_LIMIT,
};
pub(crate) use discover_record::{
    alias_discover_record_error_response, alias_discover_record_method_not_allowed,
    discover_and_record_aliases, finish_alias_discover_record_response,
    validate_alias_discover_record_body, AliasDiscoverRecordTransport,
    ALIAS_DISCOVER_RECORD_BODY_LIMIT,
};
pub(crate) use feedback::{reset_alias_feedback, validate_and_apply_feedback};
pub(crate) use feedback_read::{
    alias_feedback_read_method_not_allowed, finish_alias_feedback_read_response,
    get_alias_feedback, AliasFeedbackReadTransport, ALIAS_FEEDBACK_READ_BODY_LIMIT,
};
pub(crate) use import::{
    acquire_alias_import_permit, alias_import_error_response, alias_import_method_not_allowed,
    alias_import_success, finish_alias_import_response, import_aliases, AliasImportTransport,
    ALIAS_IMPORT_BODY_LIMIT,
};
pub(crate) use learn_apply::{
    acquire_alias_learn_apply_permit, alias_learn_apply_error_response,
    alias_learn_apply_method_not_allowed, alias_learn_apply_success,
    finish_alias_learn_apply_response, learn_and_apply_aliases, AliasLearnApplyTransport,
    ALIAS_LEARN_APPLY_BODY_LIMIT,
};
pub(crate) use read::{
    acquire_alias_read_permit, alias_read_method_not_allowed, finish_alias_read_worker,
    get_aliases, serialize_aliases, AliasReadTransport, ALIAS_READ_BODY_LIMIT,
};
