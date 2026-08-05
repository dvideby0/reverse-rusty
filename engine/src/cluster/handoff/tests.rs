use super::*;
use std::sync::atomic::AtomicBool;

use crate::events::EngineEvent;

use super::super::shard::LocalShard;

/// (shared normalizer, frozen dict, per-query `(id, Extracted, dsl)`).
type CompiledCorpus = (Arc<Normalizer>, Arc<Dict>, Vec<(u64, Extracted, String)>);

/// Compile `(id, DSL)` into a shared frozen dict + per-query `Extracted`, mirroring
/// `ClusterEngine::build`'s pass A — so a test can seed a `LocalShard` at the level the
/// coordinator uses (the same helper shape as `replica.rs`'s tests).
fn compile_corpus(dsls: &[(u64, &str)]) -> CompiledCorpus {
    let norm = Arc::new(Normalizer::default_vocab().expect("default vocabulary"));
    let mut dict = Dict::new();
    let mut lc = String::new();
    let mut out = Vec::new();
    for (id, dsl) in dsls {
        let ast = crate::dsl::parse(dsl).expect("test dsl parses");
        let ex = crate::compile::extract(&ast, &norm, &mut dict, &mut lc);
        out.push((*id, ex, (*dsl).to_string()));
    }
    dict.finalize_mask();
    (norm, Arc::new(dict), out)
}

fn local(norm: &Arc<Normalizer>, dict: &Arc<Dict>) -> LocalShard {
    let mut td = TagDict::new();
    td.mark_finalized();
    LocalShard::new(
        Arc::clone(norm),
        Arc::clone(dict),
        Arc::new(td),
        EngineConfig::default(),
    )
}

fn seed(shard: &dyn Shard, corpus: &[(u64, Extracted, String)]) {
    for (id, ex, dsl) in corpus {
        shard
            .insert_extracted_with_tags(ex, *id, 1, dsl, &[])
            .expect("seed insert");
    }
}

/// Swapping to a SET-EQUAL backing leaves matching byte-identical — ids AND stats — the core
/// "a handoff to an equivalent copy is invisible to readers" property.
#[test]
fn swap_preserves_matching_byte_identical() {
    let (norm, dict, corpus) = compile_corpus(&[(1, "alpha bravo"), (2, "charlie delta")]);
    let a = local(&norm, &dict);
    seed(&a, &corpus);
    let b = local(&norm, &dict);
    seed(&b, &corpus);

    let h = Arc::new(HandoffShard::new(Box::new(a) as Box<dyn Shard>, 0));
    let titles = ["alpha bravo zulu", "charlie delta echo", "nothing here"];
    let before: Vec<_> = titles
        .iter()
        .map(|t| {
            h.percolate_filtered(t, false, &TagPredicate::empty())
                .expect("probe")
        })
        .collect();

    h.swap_backing(Box::new(b) as Box<dyn Shard>, 1);

    for (t, exp) in titles.iter().zip(&before) {
        let got = h
            .percolate_filtered(t, false, &TagPredicate::empty())
            .expect("probe after swap");
        assert_eq!(got.0, exp.0, "ids byte-identical across swap for {t:?}");
        assert_eq!(got.1, exp.1, "stats byte-identical across swap for {t:?}");
    }
}

/// Serve-then-drop: a probe that loaded the OLD backing completes against it even after a
/// concurrent swap re-points the slot; a fresh load sees the NEW backing. Backings hold
/// DIFFERENT data so "served the old one" is observable.
#[test]
fn in_flight_read_serves_old_backing() {
    let (norm, dict, corpus) = compile_corpus(&[(1, "alpha bravo"), (2, "charlie delta")]);
    let a = local(&norm, &dict);
    seed(&a, &corpus[0..1]); // A holds only id 1
    let b = local(&norm, &dict);
    seed(&b, &corpus[1..2]); // B holds only id 2

    let h = Arc::new(HandoffShard::new(Box::new(a) as Box<dyn Shard>, 0));
    // An in-flight probe pins the old backing (A) via its loaded guard...
    let pinned = h.current.load();
    // ...a concurrent handoff re-points the slot to B...
    h.swap_backing(Box::new(b) as Box<dyn Shard>, 1);
    // ...the in-flight probe still completes against A (sees id 1, never B's id 2).
    let (ids, _) = pinned
        .percolate_filtered("alpha bravo zulu", false, &TagPredicate::empty())
        .expect("in-flight probe");
    assert!(
        ids.contains(&1),
        "in-flight read serves the OLD backing: {ids:?}"
    );
    assert!(
        !ids.contains(&2),
        "in-flight read must not see the new backing: {ids:?}"
    );

    // A fresh load now serves B: id 2 visible, A's id 1 gone.
    let (ids2, _) = h
        .percolate_filtered("charlie delta echo", false, &TagPredicate::empty())
        .expect("post-swap probe");
    assert!(
        ids2.contains(&2),
        "post-swap read serves the NEW backing: {ids2:?}"
    );
    let (ids3, _) = h
        .percolate_filtered("alpha bravo zulu", false, &TagPredicate::empty())
        .expect("post-swap probe");
    assert!(
        !ids3.contains(&1),
        "post-swap read no longer serves the old backing: {ids3:?}"
    );
}

/// The generation stamp tracks swaps and is co-visible with the new backing.
#[test]
fn generation_tracks_swaps() {
    let (norm, dict, corpus) = compile_corpus(&[(1, "alpha bravo"), (2, "charlie delta")]);
    let a = local(&norm, &dict);
    seed(&a, &corpus[0..1]);
    let b = local(&norm, &dict);
    seed(&b, &corpus[1..2]);

    let h = Arc::new(HandoffShard::new(Box::new(a) as Box<dyn Shard>, 0));
    assert_eq!(h.generation(), 0);

    h.swap_backing(Box::new(b) as Box<dyn Shard>, 7);
    assert_eq!(h.generation(), 7, "generation reflects the swap");
    // New generation and new backing are co-visible (Release/Acquire pairing).
    let (ids, _) = h
        .percolate_filtered("charlie delta echo", false, &TagPredicate::empty())
        .expect("probe");
    assert!(ids.contains(&2) && h.generation() == 7);
}

/// Readers hammering the wrapper while it is repeatedly swapped between freshly built set-equal
/// backings never observe a torn/empty read and never panic — the rayon-fan-out concurrency
/// property at unit scale (every probe must see id 1 regardless of timing).
#[test]
fn concurrent_readers_survive_swaps() {
    let (norm, dict, corpus) = compile_corpus(&[(1, "alpha bravo")]);
    let first = {
        let s = local(&norm, &dict);
        seed(&s, &corpus);
        s
    };
    let h = Arc::new(HandoffShard::new(Box::new(first) as Box<dyn Shard>, 0));
    let stop = Arc::new(AtomicBool::new(false));

    let mut readers = Vec::new();
    for _ in 0..4 {
        let h = Arc::clone(&h);
        let stop = Arc::clone(&stop);
        readers.push(std::thread::spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                let (ids, _) = h
                    .percolate_filtered("alpha bravo zulu", false, &TagPredicate::empty())
                    .expect("concurrent probe");
                assert!(
                    ids.contains(&1),
                    "set-equal backings: every probe sees id 1"
                );
            }
        }));
    }

    // Repeatedly re-point at a fresh, set-equal backing while the readers run.
    let passes = 500u64;
    for i in 0..passes {
        let s = local(&norm, &dict);
        seed(&s, &corpus);
        h.swap_backing(Box::new(s) as Box<dyn Shard>, i + 1);
    }
    stop.store(true, Ordering::Relaxed);
    for r in readers {
        r.join().expect("reader thread did not panic");
    }
    assert_eq!(h.generation(), passes);
}

/// A `Shard` that records whether `set_event_sink` (a DEFAULTED trait method) reached it, and
/// returns sentinels for two forwarded methods. The recorded flag is shared via an
/// `Arc<AtomicBool>` so the test can inspect it after the shard is boxed into the wrapper.
struct RecordingShard {
    sink_installed: Arc<AtomicBool>,
}

impl Shard for RecordingShard {
    fn percolate_filtered(
        &self,
        _t: &str,
        _b: bool,
        _pred: &TagPredicate,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        Ok((Vec::new(), MatchStats::default()))
    }
    fn percolate_filtered_ranked(
        &self,
        _t: &str,
        _b: bool,
        _pred: &TagPredicate,
        _spec: &crate::rank::CompiledRankSpec,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        Ok((Vec::new(), MatchStats::default()))
    }
    fn live_endpoints(&self) -> Vec<String> {
        vec!["http://recording:1".into()]
    }
    fn live_primary_endpoint(&self) -> Option<String> {
        Some("http://recording:1".into())
    }
    fn num_queries(&self) -> Result<usize, ShardError> {
        Ok(42) // sentinel
    }
    fn class_counts(&self) -> Result<[u64; 5], ShardError> {
        Ok([0; 5])
    }
    fn ingest_extracted(&self, _i: &[PlacedQuery]) -> Result<IngestReport, ShardError> {
        Ok(IngestReport::default())
    }
    fn insert_extracted_with_tags(
        &self,
        _e: &Extracted,
        _l: u64,
        _v: u32,
        _t: &str,
        _tags: &[(String, String)],
    ) -> Result<Option<u32>, ShardError> {
        Ok(None)
    }
    fn delete_by_logical_id(&self, _l: u64) -> Result<usize, ShardError> {
        Ok(0)
    }
    fn flush(&self) -> Result<(), ShardError> {
        Ok(())
    }
    fn seal_for_checkpoint(&self) -> Result<LogPos, ShardError> {
        Ok(LogPos(99)) // sentinel
    }
    fn segment_filenames(&self) -> Result<Vec<String>, ShardError> {
        Ok(Vec::new())
    }
    fn next_seg_id(&self) -> Result<u64, ShardError> {
        Ok(0)
    }
    fn translog_tail(&self, _from: LogPos) -> Result<Vec<(LogPos, ClusterMutation)>, ShardError> {
        Ok(Vec::new())
    }
    // DEFAULTED in the trait — override to record that the wrapper FORWARDED it.
    fn set_event_sink(&self, _sink: EventSink) {
        self.sink_installed.store(true, Ordering::Release);
    }
}

/// The wrapper forwards every method to its backing — including the DEFAULTED `set_event_sink`
/// (regression guard: relying on the trait default would silently drop the sink, so a wrapped
/// `ReplicatedShard` would never surface its degraded-redundancy events).
#[test]
fn forwards_defaulted_methods_to_backing() {
    let flag = Arc::new(AtomicBool::new(false));
    let mock = RecordingShard {
        sink_installed: Arc::clone(&flag),
    };
    let h = Arc::new(HandoffShard::new(Box::new(mock) as Box<dyn Shard>, 0));

    // Value-returning methods forward (sentinels prove they reached the backing).
    assert_eq!(h.num_queries().expect("num_queries"), 42);
    assert_eq!(h.seal_for_checkpoint().expect("seal"), LogPos(99));

    // The defaulted method forwards too (the shared flag flips on the backing).
    let sink: EventSink = Arc::new(|_ev: &EngineEvent| {});
    h.set_event_sink(sink);
    assert!(
        flag.load(Ordering::Acquire),
        "set_event_sink must FORWARD to the backing, not inherit the no-op default"
    );

    // The GC keep-set introspection forwards too (ADR-096): relying on the trait default
    // would report an EMPTY live set and let the sweep drop a slot routing still reaches.
    assert_eq!(
        h.live_endpoints(),
        vec!["http://recording:1".to_string()],
        "live_endpoints must FORWARD to the backing, not inherit the empty default"
    );
    assert_eq!(
        h.live_primary_endpoint(),
        Some("http://recording:1".to_string()),
        "live_primary_endpoint must preserve ownership instead of deriving it from keep-set order"
    );
}

/// A real write through the wrapper reaches the backing (not a no-op): insert lands and is
/// matchable, and the count reflects it.
#[test]
fn forwards_writes_to_backing() {
    let (norm, dict, corpus) = compile_corpus(&[(1, "alpha bravo"), (2, "charlie delta")]);
    let a = local(&norm, &dict);
    seed(&a, &corpus[0..1]); // start holding only id 1

    let h = Arc::new(HandoffShard::new(Box::new(a) as Box<dyn Shard>, 0));
    assert_eq!(h.num_queries().expect("count"), 1);

    let (_id, ex2, dsl2) = &corpus[1];
    h.insert_extracted_with_tags(ex2, 2, 1, dsl2, &[])
        .expect("insert via wrapper");
    assert_eq!(
        h.num_queries().expect("count"),
        2,
        "insert forwarded to backing"
    );
    assert!(h
        .percolate_filtered("charlie delta echo", false, &TagPredicate::empty())
        .expect("probe")
        .0
        .contains(&2));
}
