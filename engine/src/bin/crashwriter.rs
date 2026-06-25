//! `crashwriter` — a deliberately ungraceful durable-write worker for the
//! crash-injection harness (ADR-088, Phase 0 item 3).
//!
//! It opens an [`Engine`](reverse_rusty::segment::Engine) on `--data-dir`, then
//! runs ONE durable workload, printing a flushed, durability-gated ACK to stdout
//! after each mutation the engine has accepted. The parent test
//! (`engine/tests/crash_injection`) reads the ACK stream and **SIGKILLs this
//! process mid-workload**; every ACKed id MUST survive the kill + reopen (zero
//! false negatives). There is NO signal handler and NO flush-on-exit — death is
//! ungraceful by design (SIGKILL is uncatchable anyway). That is the whole point:
//! it exercises a real external kill mid-syscall, not a cooperative shutdown, which
//! is the gap the existing fault-injection / torn-tail simulations cannot cover.
//!
//! ## Why an ACK means "durable"
//! Each workload calls a `try_*` mutator that appends to the WAL and runs its
//! durability sync (`sync_after_append`) BEFORE returning `Ok` — so a line the
//! parent has read is a happens-before proof the write is durable (to the OS page
//! cache under the default `wal_sync_on_write=false`, which survives a process
//! SIGKILL; to disk under `RR_CRASH_FSYNC=1`, which also survives power loss).
//!
//! ## Protocol (one line each; stdout flushed after EVERY line)
//! ```text
//!   READY            engine open — the parent may begin its kill countdown
//!   ACK <id>         <id> durably inserted (its WAL sync returned) — MUST survive
//!   SKIP <id>        <id> rejected (class-D / parse) — not stored, don't-care
//!   FLUSHED          the backup workload's ingest phase is sealed — kill now lands
//!                    in a backup copy, not in the ingest
//!   WALERR <id>      a WAL append failed; the write was NOT applied — the process
//!                    exits (the parent correctly sees no ACK for <id>)
//! ```
//!
//! ## Usage
//! ```text
//!   crashwriter --data-dir DIR --queries TSV
//!               [--workload wal_append|flush|compact|backup]
//!               [--offset N] [--limit M] [--backup-dest DIR]
//!   env RR_CRASH_FSYNC=1   -> wal_sync_on_write=true (power-loss durable)
//! ```
//! `--queries` is a `id<TAB>dsl` file (one query per line) the parent writes once
//! and both sides read, so the writer and the reference oracle see byte-identical
//! queries with no regeneration drift.

use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use reverse_rusty::config::EngineConfig;
use reverse_rusty::segment::{Engine, InsertOutcome};
use reverse_rusty::{Normalizer, WriteError};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().collect();

    let Some(data_dir) = flag(&args, "--data-dir").map(PathBuf::from) else {
        eprintln!("crashwriter: --data-dir is required");
        return ExitCode::FAILURE;
    };
    let Some(queries_path) = flag(&args, "--queries") else {
        eprintln!("crashwriter: --queries TSV is required");
        return ExitCode::FAILURE;
    };
    let workload = flag(&args, "--workload").unwrap_or_else(|| "wal_append".to_string());
    let offset = flag(&args, "--offset")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    let limit = flag(&args, "--limit")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(usize::MAX);
    let backup_dest = flag(&args, "--backup-dest").map(PathBuf::from);
    // churn: always insert-then-delete the first N slice positions (the canary
    // queries the parent prepended) in addition to the every-7th rule, so a
    // resurrected delete is observable via the canaries' self-matching titles.
    let delete_prefix = flag(&args, "--delete-prefix")
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(0);
    let fsync = matches!(std::env::var("RR_CRASH_FSYNC").as_deref(), Ok("1" | "true"));

    let queries = match read_queries(&queries_path) {
        Ok(q) => q,
        Err(e) => {
            eprintln!("crashwriter: read {queries_path}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let end = offset.saturating_add(limit).min(queries.len());
    let slice: &[(u64, String)] = if offset < end {
        &queries[offset..end]
    } else {
        &[]
    };

    let norm = match Normalizer::default_vocab() {
        Ok(n) => n,
        Err(e) => {
            eprintln!("crashwriter: vocab: {e}");
            return ExitCode::FAILURE;
        }
    };
    let mut engine = match Engine::open(norm, workload_config(&workload, &data_dir, fsync)) {
        Ok(e) => e,
        Err(e) => {
            eprintln!("crashwriter: open {}: {e}", data_dir.display());
            return ExitCode::FAILURE;
        }
    };

    let stdout = io::stdout();
    let mut out = stdout.lock();
    let _ = writeln!(out, "READY");
    let _ = out.flush();

    match workload.as_str() {
        "backup" => run_backup(&mut engine, slice, backup_dest, &mut out),
        "churn" => run_churn(&mut engine, slice, delete_prefix, &mut out),
        // wal_append / flush / compact share ONE insert loop; the kill is steered
        // into a different durable window entirely by the EngineConfig (flush
        // threshold + compaction policy) chosen in `workload_config`.
        _ => run_inserts(&mut engine, slice, &mut out),
    }
}

/// Insert each query in order, emitting a flushed ACK per durably-accepted write.
/// A `flush`/`compact` config makes some of these inserts internally seal a segment
/// or trigger a merge, so a kill landing in this loop tears that durable window.
fn run_inserts(engine: &mut Engine, slice: &[(u64, String)], out: &mut impl Write) -> ExitCode {
    for (id, dsl) in slice {
        match engine.try_insert_live(dsl, *id, 1) {
            Ok(InsertOutcome::Inserted(_)) => emit(out, "ACK", *id),
            // class-D / parse rejects are not stored — the reference drops them too,
            // so they are absent on both sides (don't-care for the diff).
            Ok(InsertOutcome::RejectedClassD) | Err(WriteError::Parse(_)) => emit(out, "SKIP", *id),
            Err(WriteError::Wal(_)) => {
                // The mutation was rejected (not applied); stop — the parent sees no
                // ACK for this id, so it is correctly NOT in the must-survive set.
                emit(out, "WALERR", *id);
                return ExitCode::FAILURE;
            }
        }
    }
    // Reached the end un-killed: the corpus was too small or the kill too slow.
    // Exit cleanly; the parent asserts it actually killed (so this is not a silent
    // degrade into a graceful round-trip).
    ExitCode::SUCCESS
}

/// Insert + interleaved self-delete, to crash-test the delete-recovery path
/// (`DeleteByLogical`, ADR-066). Every 7th query is inserted and then immediately
/// deleted, emitting `TOMB <id>` ONLY after the delete is durable — so its id must
/// recover ABSENT (a resurrected delete is a false positive). The rest emit `ACK`
/// (must recover present). A query killed mid insert-or-delete emits neither, so it
/// is cleanly don't-care — no coupling to the delete rule on the parent side.
fn run_churn(
    engine: &mut Engine,
    slice: &[(u64, String)],
    delete_prefix: usize,
    out: &mut impl Write,
) -> ExitCode {
    for (i, (id, dsl)) in slice.iter().enumerate() {
        match engine.try_insert_live(dsl, *id, 1) {
            Ok(InsertOutcome::Inserted(_)) => {
                if i < delete_prefix || i % 7 == 6 {
                    if engine.delete_by_logical_id(*id).is_ok() {
                        emit(out, "TOMB", *id);
                    } else {
                        emit(out, "WALERR", *id);
                        return ExitCode::FAILURE;
                    }
                } else {
                    emit(out, "ACK", *id);
                }
            }
            Ok(InsertOutcome::RejectedClassD) | Err(WriteError::Parse(_)) => emit(out, "SKIP", *id),
            Err(WriteError::Wal(_)) => {
                emit(out, "WALERR", *id);
                return ExitCode::FAILURE;
            }
        }
    }
    ExitCode::SUCCESS
}

/// Durably ingest the slice (each ACKed) + seal it, signal `FLUSHED`, then loop
/// backups to fresh dirs forever so the process is ALWAYS inside a backup copy when
/// the parent kills it. The SOURCE `--data-dir` is what recovery reopens; a torn
/// backup must never corrupt it (and a partial dest must not be a silently-valid
/// engine). No ACK in the backup loop — the sealed inserts are the must-survive set.
fn run_backup(
    engine: &mut Engine,
    slice: &[(u64, String)],
    dest_root: Option<PathBuf>,
    out: &mut impl Write,
) -> ExitCode {
    let Some(root) = dest_root else {
        eprintln!("crashwriter: --backup-dest is required for the backup workload");
        return ExitCode::FAILURE;
    };
    for (id, dsl) in slice {
        match engine.try_insert_live(dsl, *id, 1) {
            Ok(InsertOutcome::Inserted(_)) => emit(out, "ACK", *id),
            Ok(InsertOutcome::RejectedClassD) | Err(WriteError::Parse(_)) => emit(out, "SKIP", *id),
            Err(WriteError::Wal(_)) => {
                emit(out, "WALERR", *id);
                return ExitCode::FAILURE;
            }
        }
    }
    engine.flush(); // seal the memtable into a durable base segment to copy
    let _ = std::fs::create_dir_all(&root);
    let _ = writeln!(out, "FLUSHED");
    let _ = out.flush();
    let mut i: u64 = 0;
    loop {
        // Ignore the result: a completed backup is fine, and the kill interrupts one
        // mid-copy. Fresh dest each time (backup_to refuses a pre-existing dest).
        let _ = engine.backup_to(&root.join(format!("bk_{i}")));
        i += 1;
    }
}

/// Per-workload engine config — the only thing that differs between the insert
/// scenarios, so the same loop drives a kill into the chosen durable window.
fn workload_config(workload: &str, data_dir: &Path, fsync: bool) -> EngineConfig {
    let mut cfg = EngineConfig {
        data_dir: Some(data_dir.to_path_buf()),
        wal_sync_on_write: fsync,
        ..EngineConfig::default()
    };
    match workload {
        // Pure WAL growth: no auto-flush, no compaction — the kill can only tear a
        // WAL append. `churn` rides the same config so its inserts AND its
        // `DeleteByLogical` frames stay in the WAL tail (a clean delete-replay test).
        "wal_append" | "churn" => {
            cfg.memtable_flush_threshold = usize::MAX;
            cfg.auto_compact_on_flush = false;
            cfg.auto_compact_on_ingest = false;
        }
        // Frequent flushes, no compaction — the kill tears a segment write / manifest
        // commit.
        "flush" => {
            cfg.memtable_flush_threshold = 64;
            cfg.auto_compact_on_flush = false;
            cfg.auto_compact_on_ingest = false;
        }
        // Frequent flushes that trigger merges — the kill tears a compaction's merge /
        // manifest swap / WAL reset.
        "compact" => {
            cfg.memtable_flush_threshold = 64;
            cfg.max_segments = 2;
            cfg.auto_compact_on_flush = true;
        }
        // backup: seal once, then snapshot in a loop — defaults are fine.
        _ => {
            cfg.memtable_flush_threshold = usize::MAX;
            cfg.auto_compact_on_flush = false;
        }
    }
    cfg
}

/// Write one protocol line and flush, so a line the parent reads is a happens-after
/// proof of the durable op it names (the flush makes the pipe write observable).
fn emit(out: &mut impl Write, tag: &str, id: u64) {
    let _ = writeln!(out, "{tag} {id}");
    let _ = out.flush();
}

/// The value following `name` in `--name value` argv, if present.
fn flag(args: &[String], name: &str) -> Option<String> {
    args.iter()
        .position(|a| a == name)
        .and_then(|i| args.get(i + 1))
        .cloned()
}

/// Read a `id<TAB>dsl` file into `(logical_id, dsl)` pairs. A malformed line (no
/// tab / unparseable id) is skipped — the parent writes this file, so it is
/// well-formed; the skip is belt-and-suspenders.
fn read_queries(path: &str) -> io::Result<Vec<(u64, String)>> {
    let file = std::fs::File::open(path)?;
    let mut out = Vec::new();
    for line in io::BufReader::new(file).lines() {
        let line = line?;
        if let Some((id, dsl)) = line.split_once('\t') {
            if let Ok(id) = id.parse::<u64>() {
                out.push((id, dsl.to_string()));
            }
        }
    }
    Ok(out)
}
