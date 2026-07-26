#[derive(Default, Clone, Copy, Debug, PartialEq, Eq)]
pub struct MatchStats {
    pub unique_candidates: u32, // distinct queries exact-checked
    pub postings_scanned: u32,  // total posting entries unioned (main + broad)
    /// Broad-lane subset of `postings_scanned` — the quantity the columnar batch
    /// path amortizes (each huge broad posting is scanned once per batch, not
    /// once per title). Counted on BOTH paths, so `broad_postings_scanned`
    /// columnar ÷ inline is the machine-independent amortization factor.
    pub broad_postings_scanned: u32,
    pub main_candidates: u32,
    pub broad_candidates: u32,
    pub matches: u32,
    /// Logical-id emissions after exact verification and member-level alive/tag
    /// checks, before result-level logical-id deduplication (ADR-107). This is
    /// delivery telemetry only: it never participates in matching decisions.
    pub logical_emissions: u64,
    /// Duplicate logical-id emissions removed locally or by a cluster coordinator.
    /// Kept out of compatibility response DTOs; used by the ranked-delivery baseline.
    pub duplicate_emissions: u64,
    pub probes_attempted: u32, // total signature probes (before filter)
    pub probes_skipped: u32,   // probes skipped by anchor filter (definitely-not-present)
    // ---- broad-lane batch/columnar accounting (0 on the per-title path) ----
    pub broad_queries_evaluated: u32, // distinct broad queries exact-checked via bitmap eval
    pub broad_anchors_scanned: u32,   // distinct broad anchors (postings) probed per batch
    pub broad_batches: u32,           // broad sub-batches (chunks) processed
    /// Broad candidates skipped by the batch count-gate pre-reject (lever 5a):
    /// reached + alive, but a required feature or a whole any-of group is absent
    /// from the batch, so full bitmap verification is provably pointless. The
    /// meter proving the prefilter bites; 0 with `broad_prefilter` off.
    pub broad_prefilter_skipped: u32,
    // ---- hot-tier accounting (class H, ADR-105; all 0 while θ is off) ----
    /// Hot-tier subset of `postings_scanned` — the columnar batch pass amortizes
    /// this exactly like `broad_postings_scanned` (once per batch, not per title).
    pub hot_postings_scanned: u32,
    /// Distinct hot-tier candidates reached (deduped), mirror of `broad_candidates`.
    pub hot_candidates: u32,
    /// Distinct hot-tier queries exact-checked via bitmap eval (columnar path).
    pub hot_queries_evaluated: u32,
    /// Distinct hot-tier anchors (postings) probed per batch (columnar path).
    pub hot_anchors_scanned: u32,
    /// Hot-tier columnar sub-batches processed.
    pub hot_batches: u32,
    /// Hot-tier candidates skipped by the count-gate pre-reject (lever 5a).
    pub hot_prefilter_skipped: u32,
}

impl MatchStats {
    /// Field-wise accumulate `other` into `self`. The single shared body for
    /// merging per-title stats in the parallel matchers and per-shard stats in
    /// the cluster coordinator. `matches` is summed like the rest; callers that
    /// dedup across sources (e.g. the cluster union) overwrite it afterward.
    pub fn merge(&mut self, other: MatchStats) {
        self.unique_candidates += other.unique_candidates;
        self.postings_scanned += other.postings_scanned;
        self.broad_postings_scanned += other.broad_postings_scanned;
        self.main_candidates += other.main_candidates;
        self.broad_candidates += other.broad_candidates;
        self.matches += other.matches;
        self.logical_emissions += other.logical_emissions;
        self.duplicate_emissions += other.duplicate_emissions;
        self.probes_attempted += other.probes_attempted;
        self.probes_skipped += other.probes_skipped;
        self.broad_queries_evaluated += other.broad_queries_evaluated;
        self.broad_anchors_scanned += other.broad_anchors_scanned;
        self.broad_batches += other.broad_batches;
        self.broad_prefilter_skipped += other.broad_prefilter_skipped;
        self.hot_postings_scanned += other.hot_postings_scanned;
        self.hot_candidates += other.hot_candidates;
        self.hot_queries_evaluated += other.hot_queries_evaluated;
        self.hot_anchors_scanned += other.hot_anchors_scanned;
        self.hot_batches += other.hot_batches;
        self.hot_prefilter_skipped += other.hot_prefilter_skipped;
    }

    /// Record duplicates removed by a higher-level union whose child collectors
    /// already accounted for their own emissions (for example, shard fan-in).
    pub(crate) fn record_cross_source_duplicates(&mut self, emissions: usize, unique: usize) {
        debug_assert!(unique <= emissions);
        self.duplicate_emissions = self
            .duplicate_emissions
            .saturating_add(u64::try_from(emissions.saturating_sub(unique)).unwrap_or(u64::MAX));
    }
}

/// Which broad-lane strategy a batch match uses. `Columnar` is the new
/// once-per-batch bitmap evaluator; `Inline` falls back to the original
/// per-title broad probe (`Segment::match_into(include_broad=true)`) — the
/// provable kill-switch that yields byte-identical results.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum BroadStrategy {
    Inline,
    Columnar,
}

/// Options for batch matching. Replaces the bare `include_broad: bool` on the
/// batch entry points without churning the per-title signatures.
#[derive(Clone, Copy, Debug)]
pub struct BatchMatchOptions {
    /// Evaluate the broad lane at all (default false — broad is opt-in, as on
    /// the per-title path).
    pub include_broad: bool,
    /// Title sub-batch / rayon chunk size for the columnar broad pass.
    pub broad_batch_size: usize,
    /// Columnar (new) vs Inline (original per-title broad) — the kill-switch.
    pub broad_strategy: BroadStrategy,
    /// Use the pure-anchor materialization fast path (emit pure-anchor broad
    /// queries straight from the anchor's title bitmap, skipping verification).
    /// When false, those queries go through full bitmap verification instead —
    /// identical results, slower. A kill-switch for the optimization; only
    /// consulted on the [`BroadStrategy::Columnar`] path.
    pub broad_materialize: bool,
    /// Use the batch count-gate pre-reject (lever 5a of the Broad-Query Cost
    /// Program): a reached broad candidate whose required features / any-of
    /// groups cannot all be satisfied by ANY title in the batch is skipped
    /// before full bitmap verification — a necessary-condition filter, so
    /// results are identical (under-reject is the only possible error
    /// direction). A kill-switch; only consulted on the
    /// [`BroadStrategy::Columnar`] path.
    pub broad_prefilter: bool,
}

impl Default for BatchMatchOptions {
    fn default() -> Self {
        Self {
            include_broad: false,
            broad_batch_size: 256,
            broad_strategy: BroadStrategy::Columnar,
            broad_materialize: true,
            broad_prefilter: true,
        }
    }
}

/// A batch match's per-title results paired with the aggregate [`MatchStats`] — the
/// return shape of the `match_titles_batch_with_stats*` family.
pub type BatchResultsWithStats = (Vec<(usize, Vec<u64>)>, MatchStats);

/// Typed cancellation (ADR-099/123): a cooperative deadline expired at a
/// title/segment boundary or at a bounded in-segment work poll and the match
/// was abandoned. On this error NO partial results escape —
/// every cancelled path clears its output buffer before returning, so a cancelled
/// match can never masquerade as a successful empty/short result (the zero-FN /
/// fail-loud posture). Only the `try_*` matchers armed with an explicit deadline can
/// produce it; the plain matchers are statically infallible.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MatchCancelled;

impl std::fmt::Display for MatchCancelled {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("match cancelled: cooperative deadline expired")
    }
}

impl std::error::Error for MatchCancelled {}

/// The compile-time deadline seam (ADR-099/123). Match bodies are generic over
/// this, so the unarmed monomorph ([`NoDeadline`], whose error is
/// [`Infallible`]) compiles to literally no check. Armed work uses
/// [`DeadlinePoll`] to sample the clock after a bounded number of posting,
/// candidate, anchor, or body-group operations.
pub(crate) trait DeadlineCheck: Copy {
    /// Compile-time switch used by [`DeadlinePoll`]. `false` removes both its
    /// counter mutation and clock branch from the unarmed monomorph.
    const ARMED: bool;
    type Cancelled: Send;
    fn check(self) -> Result<(), Self::Cancelled>;
}

/// The unarmed deadline: `check` is `Ok(())` with an uninhabited error, so the
/// compiler erases both the check and every `Err` arm from this monomorph.
#[derive(Clone, Copy)]
pub(crate) struct NoDeadline;

impl DeadlineCheck for NoDeadline {
    const ARMED: bool = false;
    type Cancelled = std::convert::Infallible;
    #[inline]
    fn check(self) -> Result<(), Self::Cancelled> {
        Ok(())
    }
}

/// An armed deadline: cancelled once `Instant::now()` reaches the given instant.
#[derive(Clone, Copy)]
pub(in crate::segment) struct DeadlineAt(pub(in crate::segment) std::time::Instant);

impl DeadlineCheck for DeadlineAt {
    const ARMED: bool = true;
    type Cancelled = MatchCancelled;
    #[inline]
    fn check(self) -> Result<(), Self::Cancelled> {
        if std::time::Instant::now() >= self.0 {
            Err(MatchCancelled)
        } else {
            Ok(())
        }
    }
}

/// Number of bounded work units between armed clock reads.
///
/// Lucene applies the same sampling pattern to expensive iterators (rather than
/// reading the clock for every document). 256 is small enough to bound the
/// measured dense-segment overshoot while amortizing `Instant::now()` across
/// meaningful work. The unarmed monomorph does not execute the counter at all.
pub(crate) const DEADLINE_WORK_INTERVAL: u16 = 256;

/// Request-local bounded sampler shared by every expensive loop in one match.
///
/// A work unit is one posting entry, candidate, anchor/probe, bitmap emission,
/// or canonical-body member. Parser ceilings already bound the integer program
/// inside one exact verification; this sampler bounds the unbounded
/// segment-owned collections around it.
pub(crate) struct DeadlinePoll<D: DeadlineCheck> {
    deadline: D,
    pub(in crate::segment) remaining: u16,
}

impl<D: DeadlineCheck> DeadlinePoll<D> {
    #[inline]
    pub(crate) fn new(deadline: D) -> Self {
        Self {
            deadline,
            remaining: DEADLINE_WORK_INTERVAL,
        }
    }

    /// Check at an existing title/segment boundary and restart the work budget.
    #[inline]
    pub(crate) fn check_now(&mut self) -> Result<(), D::Cancelled> {
        if D::ARMED {
            self.remaining = DEADLINE_WORK_INTERVAL;
            return self.deadline.check();
        }
        Ok(())
    }

    /// Record one in-segment work unit, sampling the deadline every fixed
    /// interval. `D::ARMED == false` is a compile-time constant, so LLVM erases
    /// this entire body for ordinary matching.
    #[inline]
    pub(crate) fn check_work(&mut self) -> Result<(), D::Cancelled> {
        if D::ARMED {
            self.remaining -= 1;
            if self.remaining == 0 {
                self.remaining = DEADLINE_WORK_INTERVAL;
                return self.deadline.check();
            }
        }
        Ok(())
    }
}

/// Deliver one scalar match while lending collectors the active request-local
/// sampler for any unbounded post-verification work. A collector that does not
/// override the poll-aware callback never invokes the closure, preserving the
/// ordinary collector's machine shape.
#[inline]
pub(crate) fn collect_match_at<C: crate::collect::MatchSink, D: DeadlineCheck>(
    collector: &mut C,
    logical_id: u64,
    local_id: u32,
    deadline: &mut DeadlinePoll<D>,
) -> Result<(), D::Cancelled> {
    let mut cancelled = None;
    collector.on_match_at_with_poll(logical_id, local_id, &mut || {
        if cancelled.is_some() {
            return true;
        }
        match deadline.check_work() {
            Ok(()) => false,
            Err(error) => {
                cancelled = Some(error);
                true
            }
        }
    });
    match cancelled {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Indexed batch counterpart to [`collect_match_at`].
#[inline]
pub(crate) fn collect_batch_match<C: crate::collect::BatchMatchSink, D: DeadlineCheck>(
    collector: &mut C,
    title_index: usize,
    logical_id: u64,
    deadline: &mut DeadlinePoll<D>,
) -> Result<(), D::Cancelled> {
    let mut cancelled = None;
    collector.on_match_with_poll(title_index, logical_id, &mut || {
        if cancelled.is_some() {
            return true;
        }
        match deadline.check_work() {
            Ok(()) => false,
            Err(error) => {
                cancelled = Some(error);
                true
            }
        }
    });
    match cancelled {
        Some(error) => Err(error),
        None => Ok(()),
    }
}

/// Unwrap a result whose error is uninhabited — the no-`unwrap()`-compliant way to
/// consume the [`NoDeadline`] monomorphs behind the existing infallible signatures.
#[inline]
pub(crate) fn infallible<T>(r: Result<T, std::convert::Infallible>) -> T {
    match r {
        Ok(v) => v,
        Err(never) => match never {},
    }
}

#[cfg(test)]
mod deadline_poll_tests {
    use super::*;
    use std::convert::Infallible;
    use std::sync::atomic::{AtomicUsize, Ordering};

    #[derive(Clone, Copy)]
    struct CountChecks<'a>(&'a AtomicUsize);

    impl DeadlineCheck for CountChecks<'_> {
        const ARMED: bool = true;
        type Cancelled = Infallible;

        fn check(self) -> Result<(), Self::Cancelled> {
            self.0.fetch_add(1, Ordering::Relaxed);
            Ok(())
        }
    }

    #[test]
    fn armed_work_sampler_checks_at_the_exact_interval() {
        let checks = AtomicUsize::new(0);
        let mut poll = DeadlinePoll::new(CountChecks(&checks));

        for _ in 0..DEADLINE_WORK_INTERVAL - 1 {
            assert!(poll.check_work().is_ok());
        }
        assert_eq!(checks.load(Ordering::Relaxed), 0);

        assert!(poll.check_work().is_ok());
        assert_eq!(checks.load(Ordering::Relaxed), 1);

        for _ in 0..DEADLINE_WORK_INTERVAL {
            assert!(poll.check_work().is_ok());
        }
        assert_eq!(checks.load(Ordering::Relaxed), 2);

        assert!(poll.check_now().is_ok());
        assert_eq!(checks.load(Ordering::Relaxed), 3);
        for _ in 0..DEADLINE_WORK_INTERVAL - 1 {
            assert!(poll.check_work().is_ok());
        }
        assert_eq!(
            checks.load(Ordering::Relaxed),
            3,
            "a boundary check must restart the bounded work interval"
        );
    }
}
