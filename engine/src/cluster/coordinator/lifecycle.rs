//! `impl ClusterEngine` — construction + durability lifecycle: initial `build`, the
//! `from_parts` assembly seam, durable-base commit, crash-recovery `open`, `checkpoint`,
//! and the `epoch` / orphan-GC helpers.
//!
//! This file is the module ROOT: it holds only the `//!` header + the `mod`
//! declarations. The methods themselves live in focused submodules, each an
//! `impl ClusterEngine` block so the type's inherent API is unchanged:
//!   - [`build`]      — `build` / `build_with_tags` (freeze the ONE dict + tag space, create `K` shards, place every query) + `commit_durable_base`
//!   - [`open`]       — `from_parts` (the shared assembly seam, also used by the distributed/gRPC builders) + `open` (reattach committed segments + replay the log tail)
//!   - [`checkpoint`] — `checkpoint` (seal shards + commit the manifest + truncate the log) + `gc_orphan_segments` + the `epoch` accessor
//!   - [`backup`]     — `backup_to` (checkpoint + snapshot the coordinator dir, ADR-079); restore is `open`

mod backup;
mod build;
mod checkpoint;
mod open;
