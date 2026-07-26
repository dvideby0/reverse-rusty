use super::{
    eval_base_lane, eval_one_segment, next_epoch, record_collection, BatchEmissionPolicy,
    BatchMatchCollector, BatchMatchOptions, BroadBatchScratch, BroadStrategy, DeadlineCheck,
    DeadlinePoll, IndexedTitleSink, Lane, MatchScratch, MatchStats, MatchView,
};

/// Match one chunk of titles: selective lane per title (unchanged), broad lane
/// once over the chunk (columnar), emitted into the chunk's indexed collector
/// (the compatibility path's per-title `outs`, or the ADR-112 per-title
/// bounded top-K slots) under the per-title emission `policy`.
/// Errs only under an armed cooperative deadline ([`DeadlineAt`], ADR-099/123) —
/// checked at title/segment boundaries plus every bounded run of columnar
/// posting/candidate/group work. On Err the collector is aborted (no partial
/// escape); the unarmed monomorph ([`NoDeadline`]) compiles the sampler away.
#[allow(clippy::too_many_arguments)] // mirrors the scratch-threading style of eval_one_segment
pub(in crate::segment::broad_batch) fn match_batch_chunk<
    D: DeadlineCheck,
    C: BatchMatchCollector,
    P: BatchEmissionPolicy,
>(
    view: &MatchView,
    titles: &[impl AsRef<str>],
    opts: BatchMatchOptions,
    ms: &mut MatchScratch,
    bs: &mut BroadBatchScratch,
    collector: &mut C,
    stats: &mut MatchStats,
    dl: D,
    policy: P,
) -> Result<(), D::Cancelled> {
    let b = titles.len();
    if b == 0 {
        return Ok(());
    }
    let mut deadline = DeadlinePoll::new(dl);
    let words = b.div_ceil(64);
    // ADR-061: the columnar kernel is single-view, so while multi-word aliases are
    // active we route the broad lane through the two-view *inline* path (`match_into`) — the
    // documented kill-switch (matching.md §4) — keeping forbidden checks recall-correct.
    // Columnar two-view is a perf follow-on; the per-title selective lane is always two-view.
    let positioned = view.has_phrase_predicates();
    let dual = view.norm.has_multiword_aliases() || positioned;
    let force_inline = dual;
    let columnar = opts.include_broad
        && !force_inline
        && matches!(opts.broad_strategy, BroadStrategy::Columnar);
    let inline_broad = opts.include_broad
        && (matches!(opts.broad_strategy, BroadStrategy::Inline) || (force_inline && !columnar));
    // The hot tier (class H, ADR-105) is ALWAYS evaluated — it is default-visible,
    // never `include_broad`-gated. The only question is WHERE: lifted into the
    // columnar pass below (the amortization the tier exists for), or inline in the
    // per-title `match_into` when columnar is unavailable (`BroadStrategy::Inline`
    // — the shared kill-switch — or the ADR-061 multi-word-alias two-view forcing).
    // Exactly one of the two runs, so no query is double-evaluated. Hot-free
    // corpora skip the lane entirely in both forms.
    let hot_present =
        view.segments.iter().any(|s| s.has_hot_entries()) || view.memtable.has_hot_entries();
    let hot_columnar =
        hot_present && !force_inline && matches!(opts.broad_strategy, BroadStrategy::Columnar);
    let hot_inline = hot_present && !hot_columnar;
    // Feature bitmaps are needed by EITHER columnar lane (the broad pass may be
    // off while the hot pass still runs — e.g. include_broad=false).
    let any_columnar = columnar || hot_columnar;

    ms.ensure(view.segments, view.memtable.len());
    bs.ensure(view.segments, view.memtable.len(), words);
    bs.feat_row.clear();
    bs.feat_bits.clear();
    bs.distinct.clear();
    bs.tmask_batch.clear();

    let n_base = view.segments.len();

    // OR of every batch title's common-mask word — the count-gate pre-reject's
    // one-AND clause (lever 5a). Folded for free while Phase 0 pushes tmasks.
    let mut batch_mask_union = 0u64;

    // ---- Phase 0: per-title normalize + selective lane + build feat bitmaps ----
    for (ti, title) in titles.iter().enumerate() {
        // Cooperative-deadline title boundary (ADR-099): abort the collector
        // before abandoning so nothing partial can be read.
        if let Err(c) = deadline.check_now() {
            collector.abort();
            return Err(c);
        }
        // per-title epoch bump for the selective lane's cross-signature dedup
        ms.epoch = ms.epoch.wrapping_add(1);
        if ms.epoch == 0 {
            for buf in &mut ms.seen {
                for v in buf.iter_mut() {
                    *v = 0;
                }
            }
            ms.epoch = 1;
        }
        let epoch = ms.epoch;
        // normalize once. The default (no active multi-word alias) takes the **single-view fast
        // path** — one feature set + one mask, no second copy (ADR-061: zero-overhead default).
        // Only with multi-word aliases active (`force_inline`) do we build the canonical `N(T)` +
        // the overlapping superset `P(T)`. Take the buffers out so we can iterate them while
        // mutating ms.seen (no aliasing, no allocation) — same trick as match_title.
        let (
            feats,
            feats_pos,
            probe_feats,
            phrase_arcs,
            phrase_arcs_pos,
            phrase_positions,
            pos_graph_complete,
        );
        if positioned {
            (phrase_positions, pos_graph_complete) = view.norm.match_phrase_views(
                title.as_ref(),
                view.dict,
                &mut ms.lc,
                &mut ms.norm,
                &mut ms.feats,
                &mut ms.feats_pos,
                &mut ms.probe_feats,
                &mut ms.phrase_arcs,
                &mut ms.phrase_arcs_pos,
            );
            feats = std::mem::take(&mut ms.feats);
            feats_pos = std::mem::take(&mut ms.feats_pos);
            probe_feats = std::mem::take(&mut ms.probe_feats);
            phrase_arcs = std::mem::take(&mut ms.phrase_arcs);
            phrase_arcs_pos = std::mem::take(&mut ms.phrase_arcs_pos);
        } else if dual {
            view.norm.match_features_dual(
                title.as_ref(),
                view.dict,
                &mut ms.lc,
                &mut ms.norm,
                &mut ms.feats,
                &mut ms.feats_pos,
            );
            feats = std::mem::take(&mut ms.feats);
            feats_pos = std::mem::take(&mut ms.feats_pos);
            probe_feats = Vec::new();
            phrase_arcs = Vec::new();
            phrase_arcs_pos = Vec::new();
            phrase_positions = 0;
            pos_graph_complete = true;
        } else {
            view.norm.match_features(
                title.as_ref(),
                view.dict,
                &mut ms.lc,
                &mut ms.norm,
                &mut ms.feats,
            );
            feats = std::mem::take(&mut ms.feats);
            feats_pos = Vec::new();
            probe_feats = Vec::new();
            phrase_arcs = Vec::new();
            phrase_arcs_pos = Vec::new();
            phrase_positions = 0;
            pos_graph_complete = true;
        }
        let neg_mask = view.title_mask(&feats);
        let tview = if positioned {
            crate::exact::TitleView::dual_positioned(
                &probe_feats,
                view.title_mask(&feats_pos),
                &feats_pos,
                phrase_positions,
                &phrase_arcs_pos,
                pos_graph_complete,
                neg_mask,
                &feats,
                phrase_positions,
                &phrase_arcs,
                &ms.phrase_match,
            )
        } else if dual {
            crate::exact::TitleView::dual(view.title_mask(&feats_pos), &feats_pos, neg_mask, &feats)
        } else {
            crate::exact::TitleView::single(neg_mask, &feats)
        };

        let lanes = crate::segment::ProbeLanes {
            include_broad: inline_broad,
            include_hot: hot_inline,
        };
        let mut cancelled = None;
        {
            let mut sink = IndexedTitleSink {
                collector: &mut *collector,
                title_index: ti,
            };
            for (i, base) in view.segments.iter().enumerate() {
                if let Err(c) = base.match_collect(
                    &tview,
                    view.dict,
                    epoch,
                    &mut ms.seen[i],
                    &mut sink,
                    lanes,
                    view.pred,
                    stats,
                    policy.title_policy(ti),
                    &mut deadline,
                ) {
                    cancelled = Some(c);
                    break;
                }
            }
            if cancelled.is_none() {
                if let Err(c) = view.memtable.match_collect(
                    &tview,
                    view.dict,
                    epoch,
                    &mut ms.seen[n_base],
                    &mut sink,
                    lanes,
                    view.pred,
                    stats,
                    policy.title_policy(ti),
                    &mut deadline,
                ) {
                    cancelled = Some(c);
                }
            }
        }
        if let Some(c) = cancelled {
            // As in the scalar path, restore every reusable title buffer before
            // abandoning the chunk. Rayon may already have scheduled another
            // chunk on this worker even though Result collection will fail.
            ms.feats = feats;
            if dual {
                ms.feats_pos = feats_pos;
            }
            if positioned {
                ms.probe_feats = probe_feats;
                ms.phrase_arcs = phrase_arcs;
                ms.phrase_arcs_pos = phrase_arcs_pos;
            }
            collector.abort();
            return Err(c);
        }

        // The columnar kernel is single-view; it only runs when no multi-word alias is
        // active (both `columnar` and `hot_columnar` are forced off otherwise), so the
        // canonical view == the superset here and the inverted index + masks are built
        // from `feats`.
        bs.tmask_batch.push(neg_mask);
        batch_mask_union |= neg_mask;
        if any_columnar {
            for &f in &feats {
                let row = if let Some(&r) = bs.feat_row.get(&f) {
                    r as usize
                } else {
                    let r = bs.feat_bits.len() / words;
                    bs.feat_bits.resize(bs.feat_bits.len() + words, 0);
                    bs.feat_row.insert(f, r as u32);
                    bs.distinct.push(f);
                    r
                };
                bs.feat_bits[row * words + (ti >> 6)] |= 1u64 << (ti & 63);
            }
        }

        ms.feats = feats; // restore the reusable buffers (positive only when it was used)
        if dual {
            ms.feats_pos = feats_pos;
        }
        if positioned {
            ms.probe_feats = probe_feats;
            ms.phrase_arcs = phrase_arcs;
            ms.phrase_arcs_pos = phrase_arcs_pos;
        }
    }

    if !any_columnar {
        record_collection(stats, collector.finish());
        return Ok(());
    }
    if columnar {
        stats.broad_batches += 1;
    }
    if hot_columnar {
        stats.hot_batches += 1;
    }

    // ---- Phase 1+2: columnar lanes (broad + hot), per segment ----
    let BroadBatchScratch {
        feat_row,
        feat_bits,
        distinct,
        tmask_batch,
        broad_seen,
        broad_epoch,
        cands,
        non_pure,
        acc,
        grp,
        member,
        choice,
    } = bs;
    let acc: &mut [u64] = &mut acc[..words];
    let grp: &mut [u64] = &mut grp[..words];
    let member: &mut [u64] = &mut member[..words];
    let choice: &mut [u64] = &mut choice[..words];
    let materialize = opts.broad_materialize;
    let prefilter = opts.broad_prefilter;

    for (si, base) in view.segments.iter().enumerate() {
        // Cooperative-deadline segment boundary in the columnar pass (ADR-099).
        if let Err(c) = deadline.check_now() {
            collector.abort();
            return Err(c);
        }
        if columnar {
            let epoch = next_epoch(broad_epoch, broad_seen);
            if let Err(c) = eval_base_lane(
                base.as_ref(),
                Lane::Broad,
                distinct,
                feat_row,
                feat_bits,
                words,
                tmask_batch,
                batch_mask_union,
                &mut broad_seen[si],
                epoch,
                cands,
                non_pure,
                acc,
                grp,
                member,
                choice,
                collector,
                materialize,
                prefilter,
                view.pred,
                stats,
                policy,
                &mut deadline,
            ) {
                collector.abort();
                return Err(c);
            }
        }
        if hot_columnar && base.has_hot_entries() {
            let epoch = next_epoch(broad_epoch, broad_seen);
            if let Err(c) = eval_base_lane(
                base.as_ref(),
                Lane::Hot,
                distinct,
                feat_row,
                feat_bits,
                words,
                tmask_batch,
                batch_mask_union,
                &mut broad_seen[si],
                epoch,
                cands,
                non_pure,
                acc,
                grp,
                member,
                choice,
                collector,
                materialize,
                prefilter,
                view.pred,
                stats,
                policy,
                &mut deadline,
            ) {
                collector.abort();
                return Err(c);
            }
        }
    }
    // memtable last (its broad_seen buffer is at index n_base)
    {
        if let Err(c) = deadline.check_now() {
            collector.abort();
            return Err(c);
        }
        if columnar {
            let epoch = next_epoch(broad_epoch, broad_seen);
            if let Err(c) = eval_one_segment(
                view.memtable,
                Lane::Broad,
                distinct,
                feat_row,
                feat_bits,
                words,
                tmask_batch,
                batch_mask_union,
                &mut broad_seen[n_base],
                epoch,
                cands,
                non_pure,
                acc,
                grp,
                member,
                choice,
                collector,
                materialize,
                prefilter,
                view.pred,
                stats,
                policy,
                &mut deadline,
            ) {
                collector.abort();
                return Err(c);
            }
        }
        if hot_columnar && view.memtable.has_hot_entries() {
            let epoch = next_epoch(broad_epoch, broad_seen);
            if let Err(c) = eval_one_segment(
                view.memtable,
                Lane::Hot,
                distinct,
                feat_row,
                feat_bits,
                words,
                tmask_batch,
                batch_mask_union,
                &mut broad_seen[n_base],
                epoch,
                cands,
                non_pure,
                acc,
                grp,
                member,
                choice,
                collector,
                materialize,
                prefilter,
                view.pred,
                stats,
                policy,
                &mut deadline,
            ) {
                collector.abort();
                return Err(c);
            }
        }
    }

    // ---- merge: dedup each title's matches across lanes + segments ----
    record_collection(stats, collector.finish());
    Ok(())
}
