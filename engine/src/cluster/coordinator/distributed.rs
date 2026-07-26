//! `impl ClusterEngine` — gRPC remote-cluster construction + cross-node operations
//! (`distributed` feature): remote / replicated assembly, peer recovery, live handoff.

use std::sync::Arc;

use crate::cluster::clog::LogPos;
use crate::cluster::handoff::{wrap_handoff, HandoffShard};
use crate::cluster::ring::HashRing;
use crate::cluster::shard::{Shard, ShardError};
use crate::dict::Dict;
use crate::events::{DurabilityOp, EngineEvent};
use crate::normalize::Normalizer;
use crate::tagdict::TagDict;

use crate::cluster::security::ClientSecurity;
use crate::cluster::transport_metrics::TransportMetrics;

use super::{ClusterConfig, ClusterDurable, ClusterEngine, ShardGroup};

mod handoff;
mod recovery;
mod remote;
mod replicated;
