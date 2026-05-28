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

pub mod error;
pub mod util;
pub mod dict;
pub mod normalize;
pub mod dsl;
pub mod compile;
pub mod config;
pub mod filter;
pub mod index;
pub mod exact;
pub mod events;
pub mod segment;
pub mod storage;
pub mod wal;
pub mod explain;
pub mod gen;
pub mod loader;
pub mod vocab;

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
