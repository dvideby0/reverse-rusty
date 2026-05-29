//! Percolator — a domain-aware reverse product-query matcher (proof of concept).
//!
//! Design: docs/design/README.md (architecture overview); CLAUDE.md (agent entry point)
//! Invariant: Lossless signature cover — if a title could positively match a
//!   query, the title must generate a signature that retrieves that query
//! Verified by: tests/oracle.rs (differential correctness oracle)
//!
//! Pipeline:
//!   raw title -> normalize -> dense feature IDs -> title signatures
//!             -> tiny candidate set -> integer-only exact verification -> matched IDs

// Library-scoped restriction lints. These encode the correctness invariants for
// *library* code only — binaries (src/bin/*) and integration tests are separate
// crate roots and do not inherit them, so they may unwrap/panic freely. The
// crate-wide pedantic + undocumented_unsafe policy lives in Cargo.toml [lints].
#![warn(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::let_underscore_must_use
)]
// Inline `#[cfg(test)]` modules live in library files, so they inherit the
// restriction lints above. Test code legitimately unwraps/panics on failed
// assertions — exempt it rather than littering tests with per-line allows.
#![cfg_attr(
    test,
    allow(
        clippy::unwrap_used,
        clippy::expect_used,
        clippy::panic,
        clippy::let_underscore_must_use
    )
)]

pub mod compile;
pub mod config;
pub mod dict;
pub mod dsl;
pub mod error;
pub mod events;
pub mod exact;
pub mod explain;
pub mod filter;
pub mod gen;
pub mod index;
pub mod loader;
pub mod normalize;
pub mod segment;
pub mod storage;
pub mod util;
pub mod vocab;
pub mod wal;

pub use compile::{CompiledQuery, CostClass};
pub use config::EngineConfig;
pub use dict::FeatureId;
pub use error::{NormalizerError, ParseError, ParseErrorKind, WriteError};
pub use events::{CompactionTrigger, EngineEvent, EngineMetrics};
pub use explain::ExplainDetail;
pub use normalize::{Normalizer, NormalizerBuilder};
pub use segment::{
    CompactionReport, Engine, EngineSnapshot, IngestItemStatus, IngestReport, InsertOutcome,
    MatchStats,
};
pub use vocab::Vocab;

// Compile-time trait assertions — these ensure key types are safe for
// multi-threaded production use (e.g. Engine behind Arc<Mutex<Engine>>,
// MatchScratch in thread-local storage, Normalizer shared read-only).
// A missing Send/Sync impl will cause a compile error here rather than
// at the call site in downstream code.
const _: () = {
    #[allow(dead_code)]
    fn assert_send<T: Send>() {}
    #[allow(dead_code)]
    fn assert_sync<T: Sync>() {}
    #[allow(dead_code)]
    fn assert_send_sync<T: Send + Sync>() {}
    #[allow(dead_code)]
    fn assertions() {
        assert_send::<Engine>();
        assert_send_sync::<EngineSnapshot>();
        assert_send::<segment::MatchScratch>();
        assert_send_sync::<Normalizer>();
        assert_send_sync::<MatchStats>();
        assert_send_sync::<EngineConfig>();
        assert_send_sync::<IngestReport>();
        assert_send_sync::<CompactionReport>();
        assert_send_sync::<InsertOutcome>();
        assert_send_sync::<IngestItemStatus>();
        assert_send_sync::<ParseError>();
        assert_send_sync::<NormalizerError>();
        assert_send_sync::<EngineEvent>();
        assert_send_sync::<EngineMetrics>();
    }
};
