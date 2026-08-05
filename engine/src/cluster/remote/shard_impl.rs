use super::{
    block_on_timeout_in_context, grpc_deadline_status, no_live_coordinator_lease_status, proto,
    ranked_rpc_err, refuse_wire_tag_ids, remaining_micros, BatchTitleRequest, CallKind,
    ClusterMutation, Duration, Extracted, FetchedMatch, IngestReport, Instant, LogPos, MatchStats,
    PlacedQuery, RemoteShard, RpcMethod, RpcOutcome, Shard, ShardBatchRankedMatch, ShardError,
    ShardRankedMatch, ShardRankedTitle, TagPredicate,
};

impl Shard for RemoteShard {
    fn percolate_filtered(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        let req = proto::PercolateRequest {
            title: title.to_string(),
            include_broad,
            // Ship the ALREADY-RESOLVED `TagId` groups (ADR-055); empty ⇒ unfiltered.
            filter: proto::tag_predicate_to_proto(pred),
            rank: None,
            shard_id: self.shard_id,
            ownership: None,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Percolate, CallKind::Read, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move { client.percolate(req).await.map(tonic::Response::into_inner) }
        })?;
        let stats = reply.stats.map(proto::stats_to_engine).unwrap_or_default();
        Ok((reply.ids, stats))
    }

    fn percolate_filtered_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
    ) -> Result<(Vec<u64>, MatchStats), ShardError> {
        self.validate_ownership(current_position, context.generation(), context.num_shards())?;
        let req = proto::PercolateRequest {
            title: title.to_string(),
            include_broad,
            filter: proto::tag_predicate_to_proto(pred),
            rank: None,
            shard_id: self.shard_id,
            ownership: Some(proto::ownership_to_proto(context)),
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Percolate, CallKind::Read, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move { client.percolate(req).await.map(tonic::Response::into_inner) }
        })?;
        if !reply.ownership_applied {
            return Err(ShardError::OwnershipMismatch(
                crate::ownership::OwnershipError::PlacementDecisionMismatch,
            ));
        }
        let stats = reply.stats.map(proto::stats_to_engine).unwrap_or_default();
        Ok((reply.ids, stats))
    }

    fn percolate_filtered_ranked(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        let req = proto::PercolateRequest {
            title: title.to_string(),
            include_broad,
            filter: proto::tag_predicate_to_proto(pred),
            // The ALREADY-COMPILED spec (ADR-075): resolved `TagId` boosts + the priority
            // key, exactly like the filter groups — the server never re-resolves strings.
            rank: Some(proto::rank_spec_to_proto(spec)),
            shard_id: self.shard_id,
            ownership: None,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::PercolateRanked, CallKind::Read, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move { client.percolate(req).await.map(tonic::Response::into_inner) }
        })?;
        // Version-skew honesty: an older server ignores the `rank` field and leaves
        // `ranked` false — fail LOUD rather than fabricate scores or silently hand the
        // caller an unranked ordering it will present as ranked.
        if !reply.ranked || reply.scores.len() != reply.ids.len() {
            return Err(ShardError::Remote(format!(
                "shard did not score a ranked percolate (ranked={}, ids={}, scores={}): \
                 the server predates cluster ranking (ADR-075) — upgrade it or drop the \
                 rank block",
                reply.ranked,
                reply.ids.len(),
                reply.scores.len()
            )));
        }
        let stats = reply.stats.map(proto::stats_to_engine).unwrap_or_default();
        Ok((reply.ids.into_iter().zip(reply.scores).collect(), stats))
    }

    fn percolate_filtered_ranked_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        spec: &crate::rank::CompiledRankSpec,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
    ) -> Result<(Vec<(u64, i64)>, MatchStats), ShardError> {
        self.validate_ownership(current_position, context.generation(), context.num_shards())?;
        let req = proto::PercolateRequest {
            title: title.to_string(),
            include_broad,
            filter: proto::tag_predicate_to_proto(pred),
            rank: Some(proto::rank_spec_to_proto(spec)),
            shard_id: self.shard_id,
            ownership: Some(proto::ownership_to_proto(context)),
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::PercolateRanked, CallKind::Read, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move { client.percolate(req).await.map(tonic::Response::into_inner) }
        })?;
        if !reply.ownership_applied || !reply.ranked || reply.scores.len() != reply.ids.len() {
            return Err(ShardError::OwnershipMismatch(
                crate::ownership::OwnershipError::PlacementDecisionMismatch,
            ));
        }
        let stats = reply.stats.map(proto::stats_to_engine).unwrap_or_default();
        Ok((reply.ids.into_iter().zip(reply.scores).collect(), stats))
    }

    fn percolate_all_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        program: Option<&crate::rank::CompiledRankProgram>,
        chunk_size: usize,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
        deadline: Option<Instant>,
        sink: &mut dyn crate::delivery::ChunkSink,
    ) -> Result<crate::delivery::ExhaustiveMatchResult, ShardError> {
        self.validate_ownership(current_position, context.generation(), context.num_shards())?;
        if chunk_size == 0 || chunk_size > crate::delivery::MAX_MATCH_CHUNK_SIZE {
            return Err(ShardError::Config(format!(
                "exhaustive chunk size {chunk_size} is outside 1..={}",
                crate::delivery::MAX_MATCH_CHUNK_SIZE
            )));
        }
        let absolute = self.bounded_deadline(deadline)?;
        let base = proto::PercolateAllRequest {
            title: title.to_string(),
            include_broad,
            filter: proto::tag_predicate_to_proto(pred),
            rank: program.map(proto::rank_program_to_proto),
            chunk_size: u32::try_from(chunk_size).unwrap_or(u32::MAX),
            remaining_micros: 0,
            shard_id: self.shard_id,
            ownership: Some(proto::ownership_to_proto(context)),
        };
        let expected_scores = program.is_some();
        let expected_profile = program.map(proto::rank_profile_identity_to_proto);
        let generation = context.generation().get();
        let num_shards = context.num_shards();
        let started = Instant::now();

        // Drive one streaming attempt. A lease-rejected call is known not to
        // have reached the handler, so it may be reclaimed and reissued before
        // the first chunk. No transport/stream failure is retried here: that
        // would splice attempts after provisional delivery.
        let result = (|| {
            const CANCEL_POLL: Duration = Duration::from_millis(10);
            let mut reclaimed = false;
            let response = loop {
                sink.check_cancelled()
                    .map_err(crate::delivery::ExhaustiveMatchError::Sink)
                    .map_err(ShardError::from)?;
                let remaining = absolute
                    .checked_duration_since(Instant::now())
                    .filter(|remaining| !remaining.is_zero())
                    .ok_or(ShardError::DeadlineExceeded)?;
                let mut body = base.clone();
                body.remaining_micros = remaining_micros(remaining);
                let mut request = tonic::Request::new(body);
                request.set_timeout(remaining);
                let mut client = self.client.clone();
                let mut response_call = Box::pin(client.percolate_all(request));
                let response = loop {
                    sink.check_cancelled()
                        .map_err(crate::delivery::ExhaustiveMatchError::Sink)
                        .map_err(ShardError::from)?;
                    let remaining = absolute
                        .checked_duration_since(Instant::now())
                        .filter(|remaining| !remaining.is_zero())
                        .ok_or(ShardError::DeadlineExceeded)?;
                    match block_on_timeout_in_context(
                        &self.handle,
                        remaining.min(CANCEL_POLL),
                        response_call.as_mut(),
                    ) {
                        Err(_) if Instant::now() >= absolute => {
                            return Err(ShardError::DeadlineExceeded);
                        }
                        Err(_) => {}
                        Ok(response) => break response,
                    }
                };
                if response
                    .as_ref()
                    .err()
                    .is_some_and(no_live_coordinator_lease_status)
                    && self.coordinator_id.is_some()
                    && !reclaimed
                {
                    self.reclaim_coordinator_lease(Some(absolute))?;
                    reclaimed = true;
                    continue;
                }
                break response;
            };
            let mut stream = match response {
                Err(status) if grpc_deadline_status(&status) => {
                    return Err(ShardError::DeadlineExceeded);
                }
                Err(status) => return Err(ranked_rpc_err(&status)),
                Ok(response) => response.into_inner(),
            };

            let mut next_sequence = 0u64;
            let mut exact_total = 0u64;
            let mut checksum = crate::delivery::DeliveryChecksum::default();
            let mut terminal: Option<crate::delivery::ExhaustiveMatchResult> = None;
            loop {
                let mut next_call = Box::pin(stream.message());
                let next = loop {
                    sink.check_cancelled()
                        .map_err(crate::delivery::ExhaustiveMatchError::Sink)
                        .map_err(ShardError::from)?;
                    let remaining = absolute
                        .checked_duration_since(Instant::now())
                        .filter(|remaining| !remaining.is_zero())
                        .ok_or(ShardError::DeadlineExceeded)?;
                    match block_on_timeout_in_context(
                        &self.handle,
                        remaining.min(CANCEL_POLL),
                        next_call.as_mut(),
                    ) {
                        Err(_) if Instant::now() >= absolute => {
                            return Err(ShardError::DeadlineExceeded);
                        }
                        Err(_) => {}
                        Ok(next) => break next,
                    }
                };
                let frame = match next {
                    Err(status) if grpc_deadline_status(&status) => {
                        return Err(ShardError::DeadlineExceeded);
                    }
                    Err(status) => return Err(ranked_rpc_err(&status)),
                    Ok(frame) => frame,
                };
                let Some(frame) = frame else {
                    break;
                };
                if terminal.is_some() {
                    return Err(ShardError::Protocol(
                        "exhaustive stream returned a frame after its summary".into(),
                    ));
                }
                match frame.frame {
                    Some(proto::percolate_all_frame::Frame::Chunk(chunk)) => {
                        if chunk.sequence != next_sequence {
                            return Err(ShardError::Protocol(format!(
                                "exhaustive chunk sequence {} arrived where {} was required",
                                chunk.sequence, next_sequence
                            )));
                        }
                        if chunk.matches.is_empty() || chunk.matches.len() > chunk_size {
                            return Err(ShardError::Protocol(format!(
                                "exhaustive chunk contains {} members; required 1..={chunk_size}",
                                chunk.matches.len()
                            )));
                        }
                        let mut members = Vec::with_capacity(chunk.matches.len());
                        for hit in chunk.matches {
                            if hit.has_score != expected_scores
                                || (!hit.has_score && hit.score != 0)
                            {
                                return Err(ShardError::Protocol(
                                    "exhaustive member score presence disagrees with the request"
                                        .into(),
                                ));
                            }
                            members.push(crate::delivery::ExhaustiveMatch {
                                logical_id: hit.logical_id,
                                score: hit.has_score.then_some(hit.score),
                            });
                        }
                        let forwarded = crate::delivery::MatchChunk {
                            sequence: chunk.sequence,
                            matches: members,
                        };
                        sink.send_chunk(&forwarded)
                            .map_err(crate::delivery::ExhaustiveMatchError::Sink)
                            .map_err(ShardError::from)?;
                        next_sequence = next_sequence.saturating_add(1);
                        exact_total = exact_total
                            .checked_add(forwarded.matches.len() as u64)
                            .ok_or_else(|| {
                                ShardError::Protocol("exhaustive total overflowed u64".into())
                            })?;
                        for member in forwarded.matches {
                            checksum.observe(member);
                        }
                    }
                    Some(proto::percolate_all_frame::Frame::Summary(summary)) => {
                        if !summary.ownership_applied
                            || summary.placement_generation != generation
                            || summary.num_shards != num_shards
                        {
                            return Err(ShardError::OwnershipMismatch(
                                crate::ownership::OwnershipError::PlacementDecisionMismatch,
                            ));
                        }
                        if summary.rank_profile.as_ref() != expected_profile.as_ref() {
                            return Err(ShardError::Protocol(
                                "exhaustive summary failed ranking-profile attestation".into(),
                            ));
                        }
                        if summary.chunk_count != next_sequence
                            || summary.exact_total != exact_total
                            || summary.checksum_xor != checksum.xor
                            || summary.checksum_sum != checksum.sum
                        {
                            return Err(ShardError::Protocol(
                                "exhaustive summary disagrees with delivered chunks".into(),
                            ));
                        }
                        terminal = Some(crate::delivery::ExhaustiveMatchResult {
                            summary: crate::delivery::ExhaustiveSummary {
                                exact_total,
                                chunk_count: next_sequence,
                                checksum,
                            },
                            stats: summary
                                .stats
                                .map(proto::stats_to_engine)
                                .unwrap_or_default(),
                        });
                    }
                    None => {
                        return Err(ShardError::Protocol(
                            "exhaustive stream returned an empty frame".into(),
                        ));
                    }
                }
            }
            terminal.ok_or_else(|| {
                ShardError::Protocol(
                    "exhaustive stream ended without its completeness summary".into(),
                )
            })
        })();

        let outcome = match &result {
            Ok(_) => RpcOutcome::Ok,
            Err(ShardError::DeadlineExceeded) => RpcOutcome::Timeout,
            Err(_) => RpcOutcome::Error,
        };
        self.metrics
            .record(RpcMethod::PercolateAll, outcome, started.elapsed(), 0);
        result
    }

    /// ADR-113: wire PIT is a named later increment — the coordinator refuses
    /// cursor requests on a remote assembly BEFORE fanning, and this explicit
    /// override keeps the refusal loud with the operator-facing alternative
    /// even if a future caller reaches the seam directly.
    fn open_pit(&self, pit: u64) -> Result<(), ShardError> {
        let _ = pit;
        Err(ShardError::PitUnsupported(
            "wire PIT is a later increment; page via an in-process cluster or single-node mode"
                .into(),
        ))
    }

    fn percolate_top_k_owned(
        &self,
        title: &str,
        include_broad: bool,
        pred: &TagPredicate,
        program: &crate::rank::CompiledRankProgram,
        options: crate::result::TopKOptions,
        context: &crate::ownership::OwnershipContext,
        current_position: u32,
        deadline: Option<Instant>,
    ) -> Result<ShardRankedMatch, ShardError> {
        self.validate_ownership(current_position, context.generation(), context.num_shards())?;
        let absolute = self.bounded_deadline(deadline)?;
        let base = proto::PercolateTopKRequest {
            title: title.to_string(),
            include_broad,
            filter: proto::tag_predicate_to_proto(pred),
            rank: Some(proto::rank_program_to_proto(program)),
            size: options.size as u32,
            track_total_hits_up_to: options.track_total_hits_up_to,
            remaining_micros: 0,
            shard_id: self.shard_id,
            ownership: Some(proto::ownership_to_proto(context)),
        };
        let client = self.client.clone();
        let reply = self.call_until(RpcMethod::PercolateTopK, absolute, move |remaining| {
            let mut client = client.clone();
            let mut body = base.clone();
            body.remaining_micros = remaining_micros(remaining);
            let mut request = tonic::Request::new(body);
            request.set_timeout(remaining);
            async move {
                client
                    .percolate_top_k(request)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        if !reply.bounded
            || !reply.ownership_applied
            || reply.requested_size != options.size as u32
            || reply.placement_generation != context.generation().get()
            || reply.num_shards != context.num_shards()
            || reply.hits.len() > options.size
            || !proto::rank_profile_identity_matches(reply.rank_profile.as_ref(), program)
        {
            return Err(ShardError::Protocol(
                "top-k reply failed bounded/ownership/configuration/profile attestation".into(),
            ));
        }
        let total_hits = reply
            .total_hits
            .map(proto::total_hits_from_proto)
            .ok_or_else(|| ShardError::Protocol("top-k reply omitted total hits".into()))?;
        let rank_stats = reply
            .rank_stats
            .map(proto::rank_stats_from_proto)
            .ok_or_else(|| ShardError::Protocol("top-k reply omitted rank stats".into()))?;
        let result_bytes =
            u64::try_from(reverse_rusty_shard_proto::encoded_len(&reply)).unwrap_or(u64::MAX);
        Ok(ShardRankedMatch {
            hits: reply
                .hits
                .into_iter()
                .map(|hit| crate::rank::RankedHit {
                    logical_id: hit.logical_id,
                    score: hit.score,
                })
                .collect(),
            total_hits,
            stats: reply.stats.map(proto::stats_to_engine).unwrap_or_default(),
            rank_stats,
            result_bytes,
        })
    }

    fn percolate_top_k_batch_owned(
        &self,
        titles: &[BatchTitleRequest<'_>],
        include_broad: bool,
        pred: &TagPredicate,
        program: &crate::rank::CompiledRankProgram,
        options: crate::result::TopKOptions,
        current_position: u32,
        deadline: Option<Instant>,
    ) -> Result<ShardBatchRankedMatch, ShardError> {
        for request in titles {
            self.validate_ownership(
                current_position,
                request.context.generation(),
                request.context.num_shards(),
            )?;
        }
        let absolute = self.bounded_deadline(deadline)?;
        let base = proto::PercolateTopKBatchRequest {
            titles: titles
                .iter()
                .map(|request| proto::BatchTitle {
                    title: request.title.to_string(),
                    ownership: Some(proto::ownership_to_proto(request.context)),
                })
                .collect(),
            include_broad,
            filter: proto::tag_predicate_to_proto(pred),
            rank: Some(proto::rank_program_to_proto(program)),
            size: options.size as u32,
            track_total_hits_up_to: options.track_total_hits_up_to,
            remaining_micros: 0,
            shard_id: self.shard_id,
        };
        // Fail loud before flight rather than through a mid-stream transport
        // error: the request must fit the same cap ceiling replies obey.
        let encoded_request = reverse_rusty_shard_proto::encoded_len(&base);
        if encoded_request > super::super::server::MAX_GRPC_RESULT_BYTES {
            return Err(ShardError::Admission(
                crate::result::TopKAdmissionError::BatchTitlesTooLarge {
                    requested: titles.len(),
                    max: crate::result::MAX_RANKED_BATCH_TITLES,
                },
            ));
        }
        let client = self.client.clone();
        let generation = self.placement_generation.get();
        let num_shards = self.num_shards;
        let expected = titles.len();
        let size = options.size as u32;
        let size_bound = options.size;
        let expected_profile = proto::rank_profile_identity_to_proto(program);
        self.call_until(RpcMethod::PercolateTopKBatch, absolute, move |remaining| {
            let mut client = client.clone();
            let mut body = base.clone();
            body.remaining_micros = remaining_micros(remaining);
            let mut request = tonic::Request::new(body);
            request.set_timeout(remaining);
            let expected_profile = expected_profile.clone();
            async move {
                use crate::cluster::ranked_wire::{attach, RankedWireCode};
                use proto::percolate_top_k_batch_frame::Frame;
                let mut stream = client.percolate_top_k_batch(request).await?.into_inner();
                // Strict in-order completeness: frame k must be title k for
                // k in 0..n, then exactly one summary with titles_served == n,
                // then end-of-stream. Anything else fails the whole batch.
                let mut titles_out: Vec<ShardRankedTitle> = Vec::with_capacity(expected);
                let mut summary_stats: Option<MatchStats> = None;
                let mut result_bytes = 0u64;
                while let Some(frame) = stream.message().await? {
                    result_bytes = result_bytes.saturating_add(
                        u64::try_from(reverse_rusty_shard_proto::encoded_len(&frame))
                            .unwrap_or(u64::MAX),
                    );
                    match frame.frame {
                        Some(Frame::Title(result)) => {
                            if summary_stats.is_some() {
                                return Err(tonic::Status::out_of_range(
                                    "batch title frame after the summary frame",
                                ));
                            }
                            if titles_out.len() >= expected {
                                return Err(tonic::Status::out_of_range(
                                    "batch stream returned more title frames than requested",
                                ));
                            }
                            if result.title_index as usize != titles_out.len() {
                                return Err(tonic::Status::out_of_range(
                                    "batch title frames arrived out of order",
                                ));
                            }
                            if !result.bounded
                                || !result.ownership_applied
                                || result.requested_size != size
                                || result.hits.len() > size_bound
                            {
                                return Err(attach(
                                    tonic::Status::failed_precondition(
                                        "batch title frame failed bounded/ownership attestation",
                                    ),
                                    RankedWireCode::Protocol,
                                    None,
                                ));
                            }
                            if result.placement_generation != generation
                                || result.num_shards != num_shards
                            {
                                return Err(attach(
                                    tonic::Status::failed_precondition(
                                        "batch title frame placement configuration mismatch",
                                    ),
                                    RankedWireCode::OwnershipMismatch,
                                    None,
                                ));
                            }
                            let total_hits = result
                                .total_hits
                                .map(proto::total_hits_from_proto)
                                .ok_or_else(|| {
                                    tonic::Status::out_of_range("title frame omitted total hits")
                                })?;
                            let rank_stats = result
                                .rank_stats
                                .map(proto::rank_stats_from_proto)
                                .ok_or_else(|| {
                                    tonic::Status::out_of_range("title frame omitted rank stats")
                                })?;
                            titles_out.push(ShardRankedTitle {
                                hits: result
                                    .hits
                                    .into_iter()
                                    .map(|hit| crate::rank::RankedHit {
                                        logical_id: hit.logical_id,
                                        score: hit.score,
                                    })
                                    .collect(),
                                total_hits,
                                rank_stats,
                            });
                        }
                        Some(Frame::Summary(summary)) => {
                            if summary_stats.is_some() {
                                return Err(tonic::Status::out_of_range(
                                    "batch stream returned a duplicate summary frame",
                                ));
                            }
                            if summary.placement_generation != generation
                                || summary.num_shards != num_shards
                            {
                                return Err(attach(
                                    tonic::Status::failed_precondition(
                                        "batch summary placement configuration mismatch",
                                    ),
                                    RankedWireCode::OwnershipMismatch,
                                    None,
                                ));
                            }
                            if summary.titles_served as usize != expected
                                || titles_out.len() != expected
                            {
                                return Err(tonic::Status::out_of_range(
                                    "batch summary disagrees with the delivered title frames",
                                ));
                            }
                            if summary.rank_profile.as_ref() != Some(&expected_profile) {
                                return Err(attach(
                                    tonic::Status::failed_precondition(
                                        "batch summary failed ranking-profile attestation",
                                    ),
                                    RankedWireCode::Protocol,
                                    None,
                                ));
                            }
                            summary_stats = Some(
                                summary
                                    .stats
                                    .map(proto::stats_to_engine)
                                    .unwrap_or_default(),
                            );
                        }
                        None => {
                            return Err(tonic::Status::out_of_range("empty batch frame"));
                        }
                    }
                }
                let Some(stats) = summary_stats else {
                    return Err(tonic::Status::out_of_range(
                        "batch stream ended without its completeness summary",
                    ));
                };
                Ok(ShardBatchRankedMatch {
                    titles: titles_out,
                    stats,
                    result_bytes,
                })
            }
        })
    }

    fn fetch_matches(
        &self,
        logical_ids: &[u64],
        max_source_bytes: usize,
        deadline: Option<Instant>,
    ) -> Result<Vec<FetchedMatch>, ShardError> {
        let absolute = self.bounded_deadline(deadline)?;
        let base = proto::FetchMatchesRequest {
            logical_ids: logical_ids.to_vec(),
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
            remaining_micros: 0,
            max_source_bytes: u64::try_from(max_source_bytes).unwrap_or(u64::MAX),
        };
        let client = self.client.clone();
        let generation = self.placement_generation.get();
        let num_shards = self.num_shards;
        let requested_rows = logical_ids.len();
        self.call_until(RpcMethod::FetchMatches, absolute, move |remaining| {
            let mut client = client.clone();
            let mut body = base.clone();
            body.remaining_micros = remaining_micros(remaining);
            let mut request = tonic::Request::new(body);
            request.set_timeout(remaining);
            async move {
                let mut stream = client.fetch_matches(request).await?.into_inner();
                let mut out = Vec::new();
                let mut remaining_bytes = max_source_bytes;
                while let Some(row) = stream.message().await? {
                    // Fail as soon as a faulty peer over-streams: tiny sources
                    // consume little byte credit, so without this cap the buffer
                    // could grow far past the requested row count until the
                    // deadline (codex review).
                    if out.len() >= requested_rows {
                        return Err(tonic::Status::out_of_range(
                            "fetch_matches stream returned more rows than requested",
                        ));
                    }
                    if row.placement_generation != generation || row.num_shards != num_shards {
                        return Err(crate::cluster::ranked_wire::attach(
                            tonic::Status::failed_precondition(
                                "fetch_matches placement configuration mismatch",
                            ),
                            crate::cluster::ranked_wire::RankedWireCode::OwnershipMismatch,
                            None,
                        ));
                    }
                    if row.source.len() > remaining_bytes {
                        return Err(crate::cluster::ranked_wire::attach(
                            tonic::Status::resource_exhausted(
                                "ranked enrichment byte credit exceeded by fetch stream",
                            ),
                            crate::cluster::ranked_wire::RankedWireCode::EnrichmentLimit,
                            Some(u64::try_from(max_source_bytes).unwrap_or(u64::MAX)),
                        ));
                    }
                    remaining_bytes -= row.source.len();
                    out.push(FetchedMatch {
                        logical_id: row.logical_id,
                        source: row.source,
                    });
                }
                Ok(out)
            }
        })
    }

    fn num_queries(&self) -> Result<usize, ShardError> {
        let client = self.client.clone();
        let shard_id = self.shard_id;
        let reply = self.call(RpcMethod::NumQueries, CallKind::Read, move || {
            let mut client = client.clone();
            async move {
                client
                    .num_queries(proto::ShardRef { shard_id })
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok(reply.count as usize)
    }

    fn live_endpoints(&self) -> Vec<String> {
        // The GC keep-set contribution (ADR-096): the endpoint this client was connected with —
        // wherever live routing reaches through this shard is a node the sweep must not drop from.
        vec![self.endpoint.clone()]
    }

    fn live_primary_endpoint(&self) -> Option<String> {
        Some(self.endpoint.clone())
    }

    fn class_counts(&self) -> Result<[u64; 5], ShardError> {
        let client = self.client.clone();
        let shard_id = self.shard_id;
        let reply = self.call(RpcMethod::ClassCounts, CallKind::Read, move || {
            let mut client = client.clone();
            async move {
                client
                    .class_counts(proto::ShardRef { shard_id })
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        let c = reply.counts;
        // The wire keeps `counts` at exactly 4 (a pre-ADR-105 reader hard-errors on
        // any other length mid-rolling-upgrade); class H rides the ADDITIVE `hot`
        // field — proto3 default-0 from an older server, invisible to older readers.
        if c.len() != 4 {
            return Err(ShardError::Remote(format!(
                "class_counts: expected 4 entries, got {}",
                c.len()
            )));
        }
        Ok([c[0], c[1], c[2], c[3], reply.hot])
    }

    fn validate_ownership(
        &self,
        position: u32,
        generation: crate::ownership::PlacementGeneration,
        num_shards: u32,
    ) -> Result<(), ShardError> {
        if position != self.shard_id {
            return Err(crate::ownership::OwnershipError::LocalPositionMissing(position).into());
        }
        if generation != self.placement_generation {
            return Err(crate::ownership::OwnershipError::GenerationMismatch {
                expected: generation,
                actual: self.placement_generation,
            }
            .into());
        }
        if num_shards != self.num_shards {
            return Err(crate::ownership::OwnershipError::ShardCountMismatch {
                expected: num_shards,
                actual: self.num_shards,
            }
            .into());
        }
        Ok(())
    }

    fn ingest_extracted(&self, items: &[PlacedQuery]) -> Result<IngestReport, ShardError> {
        refuse_wire_tag_ids(items)?;
        // Send raw DSL + raw tags, NOT the pre-extracted feature ids: the server re-compiles
        // read-only against its own frozen dict + resolves tags against its adopted frozen tag
        // space (dict-/tag-agnostic wire). The coordinator's `Extracted` was only for placement.
        let req = proto::IngestRequest {
            items: items
                .iter()
                .map(|q| proto::AddItem {
                    logical_id: q.logical,
                    dsl: q.dsl.clone(),
                    version: q.version,
                    tags: proto::tags_to_proto(&q.tags),
                    placement: Some(proto::placement_to_proto(&q.placement)),
                })
                .collect(),
            shard_id: self.shard_id,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Ingest, CallKind::Write, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move {
                client
                    .ingest_extracted(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok(IngestReport {
            ingested: reply.ingested as usize,
            rejected_parse: reply.rejected_parse as usize,
            rejected_class_d: reply.rejected_class_d as usize,
        })
    }

    fn insert_extracted_with_tags(
        &self,
        _ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
    ) -> Result<Option<u32>, ShardError> {
        let req = proto::InsertRequest {
            item: Some(proto::AddItem {
                logical_id: logical,
                dsl: text.to_string(),
                version,
                tags: proto::tags_to_proto(tags),
                placement: Some(proto::placement_to_proto(
                    &crate::ownership::QueryPlacement::standalone(),
                )),
            }),
            shard_id: self.shard_id,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Insert, CallKind::Write, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move {
                client
                    .insert_extracted(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok(reply.present.then_some(reply.local_id))
    }

    fn insert_extracted_with_placement(
        &self,
        _ex: &Extracted,
        logical: u64,
        version: u32,
        text: &str,
        tags: &[(String, String)],
        placement: &crate::ownership::QueryPlacement,
    ) -> Result<Option<u32>, ShardError> {
        placement.validate_for_shard(self.shard_id, self.placement_generation, self.num_shards)?;
        let req = proto::InsertRequest {
            item: Some(proto::AddItem {
                logical_id: logical,
                dsl: text.to_string(),
                version,
                tags: proto::tags_to_proto(tags),
                placement: Some(proto::placement_to_proto(placement)),
            }),
            shard_id: self.shard_id,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Insert, CallKind::Write, move || {
            let mut client = client.clone();
            let req = req.clone();
            async move {
                client
                    .insert_extracted(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok(reply.present.then_some(reply.local_id))
    }

    fn delete_by_logical_id(&self, logical: u64) -> Result<usize, ShardError> {
        let req = proto::DeleteRequest {
            logical_id: logical,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::Delete, CallKind::Write, move || {
            let mut client = client.clone();
            async move { client.delete(req).await.map(tonic::Response::into_inner) }
        })?;
        Ok(reply.removed as usize)
    }

    fn flush(&self) -> Result<(), ShardError> {
        let client = self.client.clone();
        let shard_id = self.shard_id;
        let placement_generation = self.placement_generation.get();
        let num_shards = self.num_shards;
        self.call(RpcMethod::Flush, CallKind::Write, move || {
            let mut client = client.clone();
            async move {
                client
                    .flush(proto::FlushRequest {
                        shard_id,
                        placement_generation,
                        num_shards,
                    })
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok(())
    }

    fn seal_for_checkpoint(&self) -> Result<LogPos, ShardError> {
        // The remote node owns its own segment durability + translog position (server-side); a
        // recovering peer learns the snapshot's position from `FetchManifest.up_to_seqno`, not
        // from this client-side call. Flush so the remote memtable seals; report `LogPos(0)` as
        // a benign sentinel (the coordinator's gRPC recovery uses the server-reported position).
        self.flush()?;
        Ok(LogPos(0))
    }

    fn segment_filenames(&self) -> Result<Vec<String>, ShardError> {
        // Never `Ok(vec![])`: a silent empty registry would drop this shard's data on a
        // future durable-remote reopen. Surface that durability is remote-side here.
        Err(ShardError::Remote(
            "segment registry is unavailable for a remote shard (durable checkpoint is \
             local-only in this increment)"
                .into(),
        ))
    }

    fn next_seg_id(&self) -> Result<u64, ShardError> {
        Err(ShardError::Remote(
            "next_seg_id is unavailable for a remote shard".into(),
        ))
    }

    fn translog_tail(&self, from: LogPos) -> Result<Vec<(LogPos, ClusterMutation)>, ShardError> {
        // Drain the source's `FetchTranslog` stream (ops > `from`) and decode each entry back
        // into a logical mutation. The coordinator replays these into the recovering target —
        // the no-quiesce catch-up (ADR-039). The tail is the small un-sealed delta.
        let req = proto::FetchTranslogRequest {
            tag_dict_fingerprint: self.tag_dict_fp,
            after_seqno: from.0,
            dict_fingerprint: self.dict_fp,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        // A long server-stream drain — no per-call deadline (keepalive-guarded), no retry
        // (the catch-up loop is the coordinator's; re-streaming mid-recovery is unsafe).
        let client = self.client.clone();
        self.call(RpcMethod::Translog, CallKind::Unbounded, move || {
            let mut client = client.clone();
            async move {
                let mut stream = client.fetch_translog(req).await?.into_inner();
                let mut out = Vec::new();
                while let Some(entry) = stream.message().await? {
                    // Fail the recovery LOUD on an undecodable frame (unset op /
                    // invalid placement), mirroring the source side's refusal to
                    // ship an unrepresentable frame: silently skipping would
                    // shorten the tail and hand back a replica missing acked
                    // writes. Unreachable from a fenced same-version peer — this
                    // is a regression tripwire, not a tolerated input.
                    let seqno = entry.seqno;
                    match proto::translog_entry_to_mutation(entry) {
                        Some(pm) => out.push(pm),
                        None => {
                            return Err(tonic::Status::internal(format!(
                                "translog entry {seqno} is undecodable (unset op or \
                                 invalid placement); refusing a shortened recovery tail"
                            )))
                        }
                    }
                }
                Ok(out)
            }
        })
    }

    // ---- translog retention leases (ADR-040) ----
    fn acquire_retention_lease(&self) -> Result<(u64, LogPos), ShardError> {
        let req = proto::RetentionLeaseRequest {
            tag_dict_fingerprint: self.tag_dict_fp,
            op: 0,
            lease_id: 0,
            pos: 0,
            dict_fingerprint: self.dict_fp,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        let client = self.client.clone();
        let reply = self.call(RpcMethod::RetentionLease, CallKind::Write, move || {
            let mut client = client.clone();
            async move {
                client
                    .retention_lease(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok((reply.lease_id, LogPos(reply.pos)))
    }

    fn renew_retention_lease(&self, lease: u64, to: LogPos) -> Result<(), ShardError> {
        let req = proto::RetentionLeaseRequest {
            tag_dict_fingerprint: self.tag_dict_fp,
            op: 1,
            lease_id: lease,
            pos: to.0,
            dict_fingerprint: self.dict_fp,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        let client = self.client.clone();
        self.call(RpcMethod::RetentionLease, CallKind::Write, move || {
            let mut client = client.clone();
            async move {
                client
                    .retention_lease(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok(())
    }

    fn release_retention_lease(&self, lease: u64) -> Result<(), ShardError> {
        let req = proto::RetentionLeaseRequest {
            tag_dict_fingerprint: self.tag_dict_fp,
            op: 2,
            lease_id: lease,
            pos: 0,
            dict_fingerprint: self.dict_fp,
            shard_id: self.shard_id,
            placement_generation: self.placement_generation.get(),
            num_shards: self.num_shards,
        };
        let client = self.client.clone();
        self.call(RpcMethod::RetentionLease, CallKind::Write, move || {
            let mut client = client.clone();
            async move {
                client
                    .retention_lease(req)
                    .await
                    .map(tonic::Response::into_inner)
            }
        })?;
        Ok(())
    }
}
