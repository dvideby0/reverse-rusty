//! `impl Segment` — the compaction merges: the mechanical [`Segment::compact_from`]
//! and the re-anchoring [`Segment::compact_from_reanchored`] (ADR-056), the latter
//! extended with the hot tier's margin-gated lane migration (ADR-105). Split out
//! of `seg.rs` (the <650-line module rule); the type definition stays in the
//! `segment` module root.

use super::Segment;
use crate::compile::{anchor_plan, CostClass, Extracted};
use crate::dict::Dict;
use crate::util::sig_key;

/// Outcome counters of a re-anchoring merge (ADR-056 + the ADR-105 migration).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ReanchorStats {
    /// Queries whose signature cover actually changed (any lane).
    pub reanchored: usize,
    /// Cross-lane moves main→hot (a θ-hot anchor left the realtime lane).
    pub hot_promoted: usize,
    /// Cross-lane moves hot→main (the anchor decayed to ≤ θ/2 — the margin gate).
    pub hot_demoted: usize,
}

impl Segment {
    /// Merge multiple source segments into one fresh segment, dropping tombstoned
    /// entries and renumbering local IDs to be dense/contiguous. This is the core
    /// compaction mechanic.
    ///
    /// Correctness argument: every alive entry is copied verbatim (exact store
    /// data, cost class); every signature posting that pointed to an alive entry
    /// is remapped to the new local ID — across ALL THREE lanes (main, broad, and
    /// the ADR-105 hot tier; dropping the hot remap would silently unanchor every
    /// class-H entry, a false negative). Dead entries are simply skipped,
    /// reclaiming their space. The resulting segment is equivalent to the union
    /// of the alive entries from all sources.
    pub fn compact_from(sources: &[&Segment]) -> Segment {
        let mut dest = Segment::new();

        for &src in sources {
            // Build the old→new local-id remap for this source segment.
            // Dead entries get u32::MAX (sentinel); alive entries get dense IDs.
            let n = src.len();
            let mut remap: Vec<u32> = vec![u32::MAX; n];
            for (old, &is_alive) in src.alive.iter().enumerate() {
                if is_alive {
                    let new_id = src.exact.copy_entry(old as u32, &mut dest.exact);
                    let logical = dest.exact.logical(new_id);
                    dest.class.push(src.class[old]);
                    dest.alive.push(true);
                    dest.alive_counter += 1;
                    dest.logical_index.entry(logical).or_default().push(new_id);
                    remap[old] = new_id;
                }
            }

            // Remap main index postings
            src.main.for_each_posting(|key, posting| {
                posting.for_each(|old_id| {
                    let new_id = remap[old_id as usize];
                    if new_id != u32::MAX {
                        dest.main.insert(key, new_id);
                    }
                });
            });

            // Remap broad index postings
            src.broad.for_each_posting(|key, posting| {
                posting.for_each(|old_id| {
                    let new_id = remap[old_id as usize];
                    if new_id != u32::MAX {
                        dest.broad.insert(key, new_id);
                    }
                });
            });

            // Remap hot index postings (ADR-105)
            src.hot.for_each_posting(|key, posting| {
                posting.for_each(|old_id| {
                    let new_id = remap[old_id as usize];
                    if new_id != u32::MAX {
                        dest.hot.insert(key, new_id);
                    }
                });
            });
        }
        // Build anchor filter for the newly compacted (sealed) segment.
        dest.build_filter();
        // Merged segment inherits the minimum epoch — still stale if any source was.
        dest.vocab_epoch = sources.iter().map(|s| s.vocab_epoch).min().unwrap_or(0);
        dest
    }

    /// Compaction's "improve" variant (ADR-056): merge like [`compact_from`](Self::compact_from)
    /// but **re-anchor** each alive query — re-derive its signature cover with the *current*
    /// feature frequencies instead of carrying the old anchors forward. Returns the merged
    /// segment plus the [`ReanchorStats`] counters.
    ///
    /// Correctness (zero false negatives): the cover is rebuilt by the SAME
    /// [`build_signatures`]/`anchor_plan` optimizer the title side is matched against
    /// (`match_into`), using the same `dict`, so any title that matches a
    /// query still generates a signature that retrieves it — the anchor choice only governs
    /// *which* posting list the query lives in. The exact-store data is copied **verbatim**
    /// (`copy_entry`), so `verify`/`is_pure_anchor` are byte-identical and forbidden features
    /// are preserved; only the index postings and the per-query cost class are re-derived.
    ///
    /// The cost class *can* change (e.g. A→B): with the common mask frozen, a feature's
    /// frequency and its hotness diverge as the corpus drifts, so a query's rarest-by-current-
    /// frequency required feature can now be a hot one and escalate to an arity-2 cover. This
    /// is still lossless because `anchor_plan`'s class-B/C anchors are always hot features, and
    /// the title side (`match_into`) generates exactly those {hot}×{other}
    /// and broad signatures — the same matched-pair guarantee. A stored non-D query is never
    /// re-anchored to class D (it always has a required/any-of feature); a stored class-D
    /// always-candidate (ADR-068) re-derives its universal broad cover verbatim.
    ///
    /// **The hot-tier migration (ADR-105)** rides this same seam, θ-aware and margin-gated:
    /// - **main→hot** (`hot_promoted`): a query whose deciding anchor's frequency has
    ///   reached θ re-derives to class H and moves into the hot index. Visibility-safe by
    ///   construction — both lanes are probed on every request.
    /// - **hot→main** (`hot_demoted`): a class-H query whose anchor decayed re-derives to
    ///   A/B, but the move is taken only when the re-derived anchor's worst frequency is
    ///   **≤ θ/2** (the hysteresis margin — an anchor oscillating around θ stays put; no
    ///   merge-to-merge thrash).
    /// - Both directions are bounded by `max_moves` per merge (the work cap): a would-be
    ///   move past the cap keeps its old cover — reader-correct at every intermediate
    ///   state, so repeated compactions converge without any single merge paying an
    ///   unbounded reorganization bill.
    ///
    /// Two transitions are **refused** outright:
    /// - **{A,B,H}→C** (the ADR-056 demote guard, extended to H): the broad lane is opt-in
    ///   (`include_broad = false` by default), so moving an always-visible query there
    ///   would hide it — a false negative. Such an entry keeps its original cover.
    /// - **C→H**: findability-*adding* (the hot tier is probed on every request), but it
    ///   would silently change which requests see the query — a documented-semantics
    ///   change, refused conservatively (a C query's top-64 anchor cannot lose its mask
    ///   bit under the frozen mask, so this arm is defensive; see ADR-105).
    ///
    /// Invariant preserved: entries are processed in ascending old-local-id order and each
    /// entry's fresh sigs are inserted at its (ascending) new id immediately, so every posting
    /// stays sorted by construction (no per-insert sort/dedup needed — same contract as
    /// `add_compiled`).
    pub fn compact_from_reanchored(
        sources: &[&Segment],
        dict: &Dict,
        theta: u32,
        max_moves: usize,
    ) -> (Segment, ReanchorStats) {
        let mut dest = Segment::new();
        let mask_inverse = dict.mask_inverse();
        let mut stats = ReanchorStats::default();
        let mut moves = 0usize;

        for &src in sources {
            // Invert the indexes once, lane-separated (old_id -> the main / broad / hot sig
            // keys it appears under), so we can tell which entries actually moved AND in
            // which lane. One pass, O(postings) — the same order as the merge, and it stands
            // in for compact_from's posting-remap passes.
            let mut old_main: Vec<Vec<u64>> = vec![Vec::new(); src.len()];
            let mut old_broad: Vec<Vec<u64>> = vec![Vec::new(); src.len()];
            let mut old_hot: Vec<Vec<u64>> = vec![Vec::new(); src.len()];
            src.main.for_each_posting(|key, posting| {
                posting.for_each(|old_id| old_main[old_id as usize].push(key));
            });
            src.broad.for_each_posting(|key, posting| {
                posting.for_each(|old_id| old_broad[old_id as usize].push(key));
            });
            src.hot.for_each_posting(|key, posting| {
                posting.for_each(|old_id| old_hot[old_id as usize].push(key));
            });

            for (old, &is_alive) in src.alive.iter().enumerate() {
                if !is_alive {
                    continue; // drop tombstoned entries, reclaiming their space
                }
                // Copy the exact-store entry verbatim (masks, forbidden, any-of, tags,
                // identity) — re-anchoring must not touch the verified semantics.
                let new_id = src.exact.copy_entry(old as u32, &mut dest.exact);
                let logical = dest.exact.logical(new_id);
                let old_class = src.class[old];

                // Re-derive the cover from the (unchanged) stored required/any-of features
                // against the current dict. `anchor_plan` reads only required + any-of, so the
                // empty `forbidden` here is irrelevant to selection.
                let (required, anyof) = dest.exact.anchoring_inputs(new_id, &mask_inverse);
                let ex = Extracted {
                    required,
                    forbidden: Vec::new(),
                    anyof,
                };
                // The pre-hash form: the margin gate below reads the re-derived
                // anchors' FREQUENCIES, which the hashed SigPlan no longer carries.
                let plan = anchor_plan(&ex, dict, theta);
                let plan_main: Vec<u64> = plan.main_anchors.iter().map(|g| sig_key(g)).collect();
                let plan_broad: Vec<u64> = plan.broad_anchors.iter().map(|g| sig_key(g)).collect();
                let plan_hot: Vec<u64> = plan.hot_anchors.iter().map(|g| sig_key(g)).collect();
                debug_assert!(
                    old_class == CostClass::D || plan.class != CostClass::D,
                    "a stored non-D query must never re-anchor to class D"
                );

                let prev_main = std::mem::take(&mut old_main[old]);
                let prev_broad = std::mem::take(&mut old_broad[old]);
                let prev_hot = std::mem::take(&mut old_hot[old]);

                // CORRECTNESS GUARD — never demote an always-visible (A/B/H) query into the
                // broad lane. The main index and the hot tier are probed on every percolate;
                // the broad lane is opt-in (the default path has `include_broad = false`), so
                // moving a query there would hide it — a false negative. A query crossing INTO
                // broad because its anchor went top-64-hot is a *hotness reclassification*,
                // which is a major-version blue/green concern (matching.md §8), NOT a silent
                // compaction change — so keep the original cover. (The reverse, broad→main,
                // only adds findability and is kept.)
                let demotes_to_broad =
                    matches!(old_class, CostClass::A | CostClass::B | CostClass::H)
                        && plan.class == CostClass::C;
                // C→H refused: findability-adding but a silent visibility-semantics change
                // (see the doc comment). Defensive under the frozen mask.
                let c_to_hot = old_class == CostClass::C && plan.class == CostClass::H;

                // The hot-tier lane moves (main↔hot), margin- and work-cap-gated (ADR-105).
                let promotes_to_hot =
                    matches!(old_class, CostClass::A | CostClass::B) && plan.class == CostClass::H;
                let demotes_from_hot =
                    old_class == CostClass::H && matches!(plan.class, CostClass::A | CostClass::B);
                let demote_margin_ok = if demotes_from_hot {
                    // Hysteresis: leave the hot tier only once the re-derived anchor's
                    // WORST frequency has fallen to θ/2 — never on a wobble around θ.
                    // θ=0 means the tier is OFF: drain unconditionally (there is no
                    // threshold to wobble around, and `worst <= 0` would otherwise
                    // strand every sealed class-H entry hot forever — codex review).
                    let worst = plan
                        .main_anchors
                        .iter()
                        .flatten()
                        .map(|&f| dict.freq(f))
                        .max()
                        .unwrap_or(0);
                    theta == 0 || worst <= theta / 2
                } else {
                    true
                };
                let lane_move = promotes_to_hot || demotes_from_hot;
                let move_allowed = !lane_move || (demote_margin_ok && moves < max_moves);

                let keep_old = demotes_to_broad || c_to_hot || !move_allowed;
                let (main_keys, broad_keys, hot_keys, class): (&[u64], &[u64], &[u64], CostClass) =
                    if keep_old {
                        (&prev_main, &prev_broad, &prev_hot, old_class)
                    } else {
                        if promotes_to_hot {
                            stats.hot_promoted += 1;
                            moves += 1;
                        } else if demotes_from_hot {
                            stats.hot_demoted += 1;
                            moves += 1;
                        }
                        (&plan_main, &plan_broad, &plan_hot, plan.class)
                    };

                for &s in main_keys {
                    dest.main.insert(s, new_id);
                }
                for &s in broad_keys {
                    dest.broad.insert(s, new_id);
                }
                for &s in hot_keys {
                    dest.hot.insert(s, new_id);
                }
                dest.class.push(class);
                dest.alive.push(true);
                dest.alive_counter += 1;
                dest.logical_index.entry(logical).or_default().push(new_id);

                // Did the cover actually change? Compare lane-tagged key sets, so a posting
                // that merely moved lane (same `u64`, different index) still counts.
                let lane_tagged = |main: &[u64], broad: &[u64], hot: &[u64]| {
                    let mut v: Vec<(u8, u64)> = main
                        .iter()
                        .map(|&k| (0u8, k))
                        .chain(broad.iter().map(|&k| (1u8, k)))
                        .chain(hot.iter().map(|&k| (2u8, k)))
                        .collect();
                    v.sort_unstable();
                    v
                };
                if lane_tagged(main_keys, broad_keys, hot_keys)
                    != lane_tagged(&prev_main, &prev_broad, &prev_hot)
                {
                    stats.reanchored += 1;
                }
            }
        }

        // Build anchor filter for the newly compacted (sealed) segment.
        dest.build_filter();
        // Merged segment inherits the minimum epoch — still stale if any source was.
        dest.vocab_epoch = sources.iter().map(|s| s.vocab_epoch).min().unwrap_or(0);
        (dest, stats)
    }
}
