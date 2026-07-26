//! [`LocalShard`] — the in-process [`Shard`] implementation.
//!
//! An owned [`Engine`] (writes serialized behind a `Mutex`) plus an
//! `ArcSwap<EngineSnapshot>` for lock-free reads, plus a per-shard durable query log
//! (the translog, ADR-039). Holds the struct, every constructor (in-memory / durable /
//! attach / self-restart), the inherent write+read helpers, the `Shard` trait impl, and
//! the clock-injectable seal core ([`LocalShard::seal_for_checkpoint_at`]).

use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use arc_swap::ArcSwap;

use crate::cluster::clog::{ClusterMutation, LogPos};
use crate::cluster::translog;
use crate::compile::{extract_readonly, Extracted};
use crate::config::EngineConfig;
use crate::dict::Dict;
use crate::events::{DurabilityOp, EngineEvent};
use crate::exact::TagPredicate;
use crate::normalize::Normalizer;
use crate::segment::{Engine, EngineSnapshot, IngestReport, MatchScratch, MatchStats, PlacedQuery};
use crate::tagdict::TagDict;

use super::retention::{resolve_lease_ttl, RetentionLeases};
use super::{EventSink, FetchedMatch, Shard, ShardError, ShardRankedMatch};

/// One in-process shard: owned engine for writes + lock-free snapshot for reads, plus a
/// per-shard durable query log (the translog, ADR-039). The translog is a no-op
/// [`NullClusterLog`](crate::cluster::clog::NullClusterLog) for an in-memory shard (byte-identical to
/// pre-ADR-039) and a CRC-framed [`FileClusterLog`](crate::cluster::clog::FileClusterLog) for a durable
/// shard (the un-sealed-write tail a recovering replica replays). Replay re-derives features
/// from the raw DSL against the frozen dict, so the caller (which always holds the shared
/// `norm`/`dict`) supplies them to [`apply_mutation`](super::apply_mutation) — the shard need not retain them.
pub(crate) struct LocalShard {
    engine: Mutex<Engine>,
    snapshot: ArcSwap<EngineSnapshot>,
    translog: Box<translog::ShardLog>,
    /// Open peer-recovery retention leases (ADR-040): while any is held, `seal_for_checkpoint`
    /// trims the translog only to `min(P, leases.floor())`, so a concurrent seal can't strand an
    /// in-flight recovery's tail. A separate `Mutex` from `engine` (lock order is always
    /// engine→retention; the lease methods take only this one).
    retention: Mutex<RetentionLeases>,
    /// Retention-lease TTL (ADR-048): a lease that has not heartbeated within this window is
    /// reaped at the next `seal_for_checkpoint`, so a crashed recovery can no longer pin the
    /// tail forever. `None` ⇒ disabled (a lease never expires — byte-identical to ADR-040).
    /// Derived once at construction from `EngineConfig::retention_lease_ttl_secs`.
    retention_lease_ttl: Option<Duration>,
    /// Optional event sink (ADR-021), installed by the coordinator's `set_observer`. A plain
    /// `LocalShard` emitted nothing before ADR-048; now it surfaces a TTL lease reap so an
    /// abandoned recovery is observable rather than silent. `None` ⇒ no observer (events
    /// dropped — byte-identical default path; a reap only fires at checkpoint time, long after
    /// an observer would have attached at cluster build/open).
    event_sink: Mutex<Option<EventSink>>,
    /// Retained for translog replay (re-derive features from raw DSL) on self-restart, and to
    /// stamp the per-shard checkpoint sidecar's dict fingerprint.
    norm: Arc<Normalizer>,
    dict: Arc<Dict>,
    /// `Some` ⇒ durable (segments + translog + checkpoint sidecar live here); `None` ⇒ in-memory.
    data_dir: Option<PathBuf>,
    /// Pinned point-in-time snapshots keyed by the coordinator-allocated pit id
    /// (ADR-113). TTL/caps live at the coordinator (whose registry owns the
    /// lifecycle and fans `close_pit`); this map only holds the pins. Dropped
    /// with the shard — a resize/set_vocab rebuild releases every pin, and the
    /// coordinator's generation gate 409s the cursor before it ever gets here.
    pits: Mutex<crate::util::FastMap<u64, Arc<EngineSnapshot>>>,
}

mod events;
mod mutations;
mod open;
mod ranked;
mod shard_impl;
