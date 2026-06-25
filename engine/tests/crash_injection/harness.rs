//! The crash-injection harness: generate a corpus, spawn the `crashwriter` on it,
//! SIGKILL it mid-workload, reopen the data dir in-process, and diff the recovered
//! engine against the front-end-INDEPENDENT reference matcher (ADR-087).
//!
//! The verdict NEVER depends on where the kill lands — the ACK stream the parent
//! read defines exactly which ids must survive (zero false negatives); the kill
//! timing is jittered only to sweep the durable window across iterations.

#![allow(dead_code)] // some helpers are used by only a subset of the scenario modules

use std::collections::{HashMap, HashSet};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

use reverse_rusty::config::EngineConfig;
use reverse_rusty::gen::{generate, GenConfig};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};
use reverse_rusty_ref_matcher::{RefMatcher, RefVocab};

/// Kill/reopen cycles per scenario. Small by default (each spawns + SIGKILLs a real
/// process and does real fsyncs); a nightly soak bumps `RR_CRASH_ITERS`.
pub fn crash_iters() -> usize {
    std::env::var("RR_CRASH_ITERS")
        .ok()
        .and_then(|v| v.parse().ok())
        .filter(|&n| n > 0)
        .unwrap_or(3)
}

/// A collision-free temp dir (pid + a process-local counter), like the persistence
/// suite's `test_dir` — the crash suite runs alongside the other test binaries.
pub fn unique_dir(name: &str) -> PathBuf {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let n = COUNTER.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("rr_crash_{name}_{}_{n}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).expect("create crash test dir");
    dir
}

/// Deterministic-but-varied per-iteration jitter, so the kill interrupts a different
/// op instance each cycle without making the verdict nondeterministic.
pub fn jitter_for(iter: usize) -> Duration {
    Duration::from_micros((iter as u64 * 263 + 50) % 3000)
}

/// A generated corpus + the on-disk `id<TAB>dsl` file BOTH the writer and the
/// reference read, so they see byte-identical queries with no regeneration drift.
pub struct Corpus {
    pub queries: Vec<(u64, String)>,
    pub titles: Vec<String>,
    pub tsv: PathBuf,
    dsl: HashMap<u64, String>,
}

impl Corpus {
    /// Generate a dense corpus (small player/set pools) so even a few-thousand-query
    /// acked PREFIX — all the writer inserts before the kill — still produces matches,
    /// keeping the diff non-degenerate.
    pub fn generate(name: &str, seed: u64, num_queries: usize, num_titles: usize) -> Self {
        Self::generate_with_canaries(name, seed, num_queries, num_titles, 0)
    }

    /// As [`generate`](Self::generate), but PREPEND `n_canaries` self-matching canary
    /// queries (a unique two-token query plus a title of the same two tokens) at the
    /// front of the corpus. The churn writer (`--delete-prefix n_canaries`) deletes
    /// exactly these, so a resurrected delete is observable: a recovered canary
    /// matches its title, and since canaries are tombed they are excluded from the FP
    /// reference — a resurrection therefore fires as a false positive. Their tokens
    /// (`zzcanaryNN…`) collide with nothing in the generated vocabulary.
    pub fn generate_with_canaries(
        name: &str,
        seed: u64,
        num_queries: usize,
        num_titles: usize,
        n_canaries: usize,
    ) -> Self {
        let cfg = GenConfig {
            num_queries,
            num_titles,
            broad_query_frac: 0.06,
            hot_skew: 2.0,
            family_size: 8,
            seed,
            num_players: 1_500,
            num_sets: 600,
        };
        let data = generate(&cfg);
        let mut queries: Vec<(u64, String)> = Vec::with_capacity(n_canaries + data.queries.len());
        let mut titles = data.titles;
        for c in 0..n_canaries {
            let text = format!("zzcanary{c}alpha zzcanary{c}bravo");
            queries.push((9_000_000 + c as u64, text.clone()));
            titles.push(text); // the query's own tokens as a title => a self-match
        }
        queries.extend(data.queries);

        let dir = unique_dir(&format!("corpus_{name}"));
        let tsv = dir.join("queries.tsv");
        let mut f = std::fs::File::create(&tsv).expect("create queries.tsv");
        for (id, dsl) in &queries {
            // gen DSL + canary text contain no tab or newline, so the framing is safe.
            writeln!(f, "{id}\t{dsl}").expect("write tsv line");
        }
        f.sync_all().ok();
        let dsl = queries.iter().map(|(id, q)| (*id, q.clone())).collect();
        Corpus {
            queries,
            titles,
            tsv,
            dsl,
        }
    }
}

/// When the parent fires the SIGKILL.
#[derive(Clone, Copy)]
pub enum Trigger {
    /// After READY and at least N ACKs (the insert/upsert scenarios — kill lands in
    /// the insert/flush/compaction/upsert window).
    Acks(usize),
    /// After READY and at least N TOMBs (the watermark scenario — kill lands after
    /// the post-reopen canary delete is durable, leaving it unsealed in the WAL tail).
    Tombs(usize),
    /// After the writer signals FLUSHED (the backup scenario — kill lands in a
    /// backup copy, not in the ingest phase).
    Flushed,
}

/// What the parent observed: the ids it READ an ACK for before killing (the
/// must-survive set), the ids it saw TOMB for (durably inserted-then-deleted — the
/// must-be-ABSENT set, the churn scenario), and whether it delivered the kill.
pub struct AckResult {
    pub acked: Vec<u64>,
    pub tombed: Vec<u64>,
    pub killed: bool,
}

/// Spawn the `crashwriter`, read its ACK stream until `trigger`, then (after
/// `jitter`) deliver a real SIGKILL and reap. Returns the ids ACKed before the kill.
#[allow(clippy::too_many_arguments)]
pub fn spawn_and_kill(
    workload: &str,
    data_dir: &Path,
    tsv: &Path,
    extra_args: &[String],
    fsync: bool,
    trigger: Trigger,
    jitter: Duration,
) -> AckResult {
    let mut child = Command::new(env!("CARGO_BIN_EXE_crashwriter"))
        .arg("--data-dir")
        .arg(data_dir)
        .arg("--queries")
        .arg(tsv)
        .arg("--workload")
        .arg(workload)
        .args(extra_args)
        .env("RR_CRASH_FSYNC", if fsync { "1" } else { "0" })
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .expect("spawn crashwriter");
    let mut reader = BufReader::new(child.stdout.take().expect("piped stdout"));

    let mut acked = Vec::new();
    let mut tombed = Vec::new();
    let mut ready = false;
    let mut flushed = false;
    let mut line = String::new();
    loop {
        line.clear();
        match reader.read_line(&mut line) {
            // EOF: the writer exited before we killed it (clean finish or WALERR).
            Ok(0) => {
                let _ = child.wait();
                return AckResult {
                    acked,
                    tombed,
                    killed: false,
                };
            }
            Ok(_) => {}
            Err(_) => {
                let _ = child.kill();
                let _ = child.wait();
                return AckResult {
                    acked,
                    tombed,
                    killed: true,
                };
            }
        }
        match line.trim_end() {
            "READY" => ready = true,
            "FLUSHED" => flushed = true,
            l => {
                if let Some(id) = l.strip_prefix("ACK ").and_then(|r| r.parse::<u64>().ok()) {
                    acked.push(id);
                } else if let Some(id) = l.strip_prefix("TOMB ").and_then(|r| r.parse::<u64>().ok())
                {
                    tombed.push(id);
                }
            }
        }
        let fire = match trigger {
            Trigger::Acks(k) => ready && acked.len() >= k,
            Trigger::Tombs(k) => ready && tombed.len() >= k,
            Trigger::Flushed => flushed,
        };
        if fire {
            if !jitter.is_zero() {
                std::thread::sleep(jitter);
            }
            let _ = child.kill(); // real SIGKILL on Unix (std sends SIGKILL)
            let _ = child.wait();
            return AckResult {
                acked,
                tombed,
                killed: true,
            };
        }
    }
}

/// The reference over the FULL corpus — built ONCE per scenario and reused across
/// iterations (it never changes), so the per-iteration cost is just the small
/// acked-only reference + the diff.
pub fn full_reference(corpus: &Corpus) -> RefMatcher {
    RefMatcher::build(&corpus.queries, RefVocab::default_vocab())
}

/// The reference over only the first `n` queries — for a scenario whose writer
/// inserts a BOUNDED slice (`--limit n`). A recovered id beyond that slice could
/// never have been written, so it must be a false positive, not silently allowed by
/// a full-corpus reference (the backup scenario, which seals a fixed `--limit`).
pub fn full_reference_prefix(corpus: &Corpus, n: usize) -> RefMatcher {
    let n = n.min(corpus.queries.len());
    RefMatcher::build(&corpus.queries[..n], RefVocab::default_vocab())
}

/// Reopen the data dir in-process and assert the recovered engine is byte-identical
/// to the INDEPENDENT reference over the acked set: zero false negatives (every
/// acked query's matches present) + zero false positives (no match outside the full
/// corpus's legitimate matches). `full_ref` is the [`full_reference`] for this
/// corpus. Returns the total acked-truth count (for the non-degeneracy aggregate).
#[allow(clippy::too_many_arguments)]
pub fn reopen_and_diff(
    data_dir: &Path,
    corpus: &Corpus,
    full_ref: &RefMatcher,
    acked: &[u64],
    tombed: &[u64],
    fsync: bool,
    label: &str,
) -> usize {
    let cfg = EngineConfig {
        data_dir: Some(data_dir.to_path_buf()),
        wal_sync_on_write: fsync,
        ..EngineConfig::default()
    };
    let engine = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg)
        .unwrap_or_else(|e| panic!("[{label}] reopen failed (recovery must not error): {e}"));

    let acked_set: HashSet<u64> = acked.iter().copied().collect();
    let acked_queries: Vec<(u64, String)> = corpus
        .queries
        .iter()
        .filter(|(id, _)| acked_set.contains(id))
        .cloned()
        .collect();
    assert!(
        !acked_queries.is_empty(),
        "[{label}] the writer made no durable progress before the kill"
    );
    // FN reference: ONLY the durably-acked queries — every match here MUST appear.
    let acked_ref = RefMatcher::build(&acked_queries, RefVocab::default_vocab());

    // FP reference: the full corpus MINUS the durably-tombed (inserted-then-deleted)
    // ids, so a resurrected delete — an id recovery should have removed — surfaces as
    // a false positive. With no tombs (the insert-only scenarios) this is the cached
    // corpus-wide reference; the churn scenario rebuilds it per iteration.
    let full_owned;
    let full: &RefMatcher = if tombed.is_empty() {
        full_ref
    } else {
        let tomb_set: HashSet<u64> = tombed.iter().copied().collect();
        let kept: Vec<(u64, String)> = corpus
            .queries
            .iter()
            .filter(|(id, _)| !tomb_set.contains(id))
            .cloned()
            .collect();
        full_owned = RefMatcher::build(&kept, RefVocab::default_vocab());
        &full_owned
    };

    let mut scratch = MatchScratch::new();
    let mut out = Vec::new();
    let (mut fneg, mut fpos, mut acked_truth_total) = (0usize, 0usize, 0usize);
    let mut samples: Vec<String> = Vec::new();
    for title in &corpus.titles {
        engine.match_title(title, &mut scratch, &mut out, /* include_broad */ true);
        let engine_set: HashSet<u64> = out.iter().copied().collect();
        let acked_truth = acked_ref.matches(title);
        let full_truth = full.matches(title);
        acked_truth_total += acked_truth.len();
        for t in &acked_truth {
            if !engine_set.contains(t) {
                fneg += 1;
                if samples.len() < 10 {
                    samples.push(format!(
                        "FN q#{t} {:?} | title {title:?}",
                        corpus.dsl.get(t)
                    ));
                }
            }
        }
        for e in &engine_set {
            if !full_truth.contains(e) {
                fpos += 1;
                if samples.len() < 10 {
                    samples.push(format!(
                        "FP q#{e} {:?} | title {title:?}",
                        corpus.dsl.get(e)
                    ));
                }
            }
        }
    }
    eprintln!(
        "[{label}] acked={} acked_truth={acked_truth_total} FN={fneg} FP={fpos}",
        acked.len()
    );
    for s in &samples {
        eprintln!("    {s}");
    }
    assert_eq!(
        fneg, 0,
        "[{label}] FALSE NEGATIVES — a durably-acked query is missing after kill+reopen (cardinal sin)"
    );
    assert_eq!(
        fpos, 0,
        "[{label}] false positives — engine matched what no real query allows (corruption)"
    );
    acked_truth_total
}

/// Build a durable base `data_dir` in-process: insert each query (class-D / parse
/// rejects silently dropped, exactly as a default engine does) then `flush()` —
/// sealing one base segment AND committing the manifest's `wal_seq_watermark` (the
/// last WAL seq) while resetting the WAL. The shared setup for the upsert + watermark
/// scenarios: it puts the OLD versions (upsert) / the to-be-deleted canary
/// (watermark) on disk as a flushed base BEFORE the crash-tested worker runs, and —
/// for watermark — leaves a non-zero watermark the worker's post-reopen delete must
/// out-rank. The flush resets the WAL, so the worker reopens onto a clean tail.
pub fn build_base(data_dir: &Path, queries: &[(u64, String)]) {
    let cfg = EngineConfig {
        data_dir: Some(data_dir.to_path_buf()),
        memtable_flush_threshold: usize::MAX, // one memtable, one explicit flush
        auto_compact_on_flush: false,
        auto_compact_on_ingest: false,
        ..EngineConfig::default()
    };
    let mut engine =
        Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("build_base: open");
    for (id, dsl) in queries {
        // Ignore the outcome: a class-D / parse reject is dropped here exactly as the
        // independent reference (`RefMatcher::build`) drops it, so both sides agree.
        let _ = engine.try_insert_live(dsl, *id, 1);
    }
    engine.flush(); // seal the base + commit wal_seq_watermark + reset the WAL
}
