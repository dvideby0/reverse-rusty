//! `impl Engine` — construction, configuration, vocabulary, crash recovery
//! (`open`), the observer hook, and engine-handle accessors. Heavier lifecycle
//! work (ingest, flush/compaction, persistence) lives in sibling submodules.
//!
//! This file is the module ROOT: it holds only the submodule declarations. The
//! `impl Engine` blocks are split across focused sub-submodules so each concern is
//! self-contained (every block is an inherent `impl Engine`, so the type's public
//! API is identical regardless of which file each method sits in — no re-exports
//! are needed):
//!   - [`construct`] — the builders (`new`/`with_config`/`with_shared`/`with_vocab`/
//!     `with_shared_segments_only`) + the shared data-dir initializers
//!   - [`recovery`]  — reopen: `open` (skip-corrupt + WAL replay) and
//!     `open_shared_segments` (cluster-shard attach, fail-loud)
//!   - [`vocab`]     — runtime vocabulary: `set_vocab`/`adopt_vocab`, stale-epoch
//!     bookkeeping, live sources/tags, `recompile_stale_segments`, learn-and-apply
//!   - [`accessors`] — observer hook, config get/set, `snapshot`, and the read-only
//!     engine-handle accessors (dict / normalizer / segment filenames / explain)
//!   - [`backup`]    — `backup_to`: a consistent on-disk snapshot of `data_dir`
//!     (ADR-079); restore is `open`

mod accessors;
mod backup;
mod construct;
mod recovery;
mod vocab;
