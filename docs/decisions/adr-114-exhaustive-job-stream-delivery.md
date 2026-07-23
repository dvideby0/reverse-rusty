# ADR-114: Exhaustive job and stream delivery

- **Status:** Accepted
- **Date:** 2026-07-22

## Context

ADR-107 deliberately made `result_mode=all` a named contract but did not expose it on the
bounded v2 serving surface. A complete match set can be much larger than a safe HTTP or gRPC
message: the synthetic broad-heavy capture already produces thousands of true matches for one
title, and a real tenant can legitimately install far more. Raising a message limit or building
the whole `Vec<u64>` before pagination moves the same unbounded allocation to a different layer.

The ranked-percolation program's Increment 7 requires a bounded `ChunkCollector`, HTTP job
control, a server-streaming shard protocol, terminal completion/failure semantics, at-least-once
idempotency, and separate admission. The correctness wrinkle is local physical duplication:
legacy `insert_live` and direct bulk-ingest callers may leave more than one live row with the same
logical id. Compatibility collection sorts and deduplicates the complete vector. A chunker that
emits those rows directly cannot report an exact unique total without retaining an unbounded set.

## Decision

### Exact chunks are provisional; one completion record commits the set

`POST /_percolate/jobs` accepts one document, `result_mode=all`, an explicit visibility scope,
an event id, and a stream sink. It returns `202 Accepted` and a job id. `GET
/_percolate/jobs/{id}` reports lifecycle state; `GET /_percolate/jobs/{id}/stream` is a
single-consumer NDJSON stream of:

```text
match_chunk(job_id, sequence, members[])
completion(job_id, exact_total, snapshot_generation, checksum)
```

Every member carries an idempotency key derived with SHA-256 from
`(event_id, snapshot_generation, logical_id)`. Chunk sequence starts at zero and is contiguous.
`snapshot_generation` is an opaque `u64` allocated from a random boot namespace followed by a
monotonic process-local sequence. A restarted server therefore does not restart the idempotency
namespace at 1 and cannot cause a durable consumer to collapse a new attempt into provisional
members left by the prior process.
The checksum is an order-independent pair of independent stable 64-bit accumulators over unique
`(logical_id, optional_score)` members, so a consumer can verify the set after idempotency
deduplication without relying on chunk or shard order. Score presence is domain-separated
independently in both accumulators: `None` is never represented as a score sentinel that a valid
`Some(i64)` value could reproduce.

Chunks are provisional. Only `completion` commits a result. A cancellation, deadline, sink
failure, shard error, ownership/version mismatch, or truncated stream marks the job failed and
must never emit completion. Retrying a sink publish is at least once: the same member key may be
observed repeatedly and is safe to collapse. The engine does not claim broker-side exactly-once
effects.
The HTTP job does not enter `completed` when its terminal bytes are merely accepted by the bounded
MPSC queue. The worker waits until the stream dequeues that frame; only that dequeue atomically
attests completion and publishes the exact summary to status. If a claimed response is dropped
while completion is still queued, the queued frame is invalidated and the job fails with no
summary. Thus status can never certify a terminal record that the single consumer had no
opportunity to observe.
Cancellation, deadline expiry, and terminal dequeue linearize through one terminal-state gate.
Whichever transition wins is final: a cancellation or expiry already accepted by the gate cannot
be overwritten by a concurrent dequeue, and a completion already delivered makes a later
cancellation request a no-op. The same first-transition rule applies to every invalidation:
dropping an undelivered terminal frame records `not consumed`, for example, and a later DELETE
cannot rewrite that forensic cause to `cancelled`.

### Collection stays bounded and preserves logical-id semantics

The lean library exposes `ChunkSink`, `MatchChunk`, `ExhaustiveMatch`, `DeliveryChecksum`, and
the snapshot exhaustive entry point. `ChunkCollector` is another monomorphized
post-verification collector beside `AllCollector` and `TopKCollector`: it preallocates one
fixed-capacity member buffer, hands a full chunk synchronously to the sink, and retains only that
buffer plus counters/checksum. Each flush temporarily moves that same allocation into the borrowed
frame and recovers it after the synchronous sink call; there is no allocation per chunk.
Backpressure therefore reaches the matching worker at chunk boundaries. No strings, AST work,
allocation, or virtual dispatch enters exact verification.

`ChunkSink::check_cancelled` is the out-of-band companion to `send_chunk`. The exhaustive
collector polls it before title normalization/deduper allocation and at probe/candidate
boundaries, including inside a posting and before filtering every member of a canonical-body
dedup group. A cancelled job, disconnected consumer, expired deadline, or already-failed sink
therefore stops a zero-result/below-one-chunk match, expensive setup, a large
all-dead/filtered/suppressed body group, or a large posting without waiting for another emission.
The exhaustive duplicate predicate and newest-live ranked-metadata lookup receive the same poll
hook and check it between legacy physical copies in their reverse-index walks, so a pathological
many-version logical id cannot hold the cluster mutation barrier past cancellation or deadline.
Existing sinks inherit an infallible default; non-exhaustive collectors retain the
statically-false stop path.

The collector receives the physical `(segment ordinal, local id)` of each verified member.
Before accepting one, its exhaustive-only deduper checks whether an earlier physical copy of the
same logical id also satisfies the already-normalized title, visibility scope, tag predicate, and
distributed emission policy. Only the deterministic first *matching* copy is collected. This:

- preserves compatibility union semantics when two live versions have different query bodies;
- produces one unique logical member and an exact total/checksum without a result-sized set;
- is bounded by segment metadata already resident in the snapshot;
- adds duplicate-check work only to exhaustive jobs. Existing compatibility/top-K collectors use
  the default physical callback and compile to their prior behavior.

The common case has one live physical row per logical id (the REST `PUT` path is atomic upsert and
the cluster enforces a unique logical-id directory). The extra reverse-index lookups are accepted
for this background, exhaustive mode; emission volume and sink backpressure dominate its intended
workloads.

### Distributed delivery is ownership-disjoint and fail closed

`Shard` gains an ownership-aware exhaustive callback. `LocalShard` runs the same
`ChunkCollector`; `RemoteShard` consumes a new server-streaming `PercolateAll` RPC. Each stream
contains contiguous bounded chunk frames followed by exactly one summary that attests ownership,
placement generation, shard count, unique total, chunk count, and checksum. The client validates
all of them and treats EOF before the summary, a frame after it, or an oversized/out-of-sequence
frame as a protocol failure. A shard request with `include_broad=true` is admitted only when that
shard is the ownership context's named broad evaluator; a missing or different evaluator fails
before worker spawn, rather than silently omitting replicated-broad matches from a self-consistent
summary.

The coordinator derives one ADR-109 `OwnershipContext` and evaluates the broad lane on its one
broad evaluator exactly as top-K does. It forwards routed shards through one resequencing sink and
merges totals/checksums. Ownership makes shard member sets disjoint; a duplicate logical id from
two shard summaries is an ownership violation, not something to hide with coordinator dedup.
That proof assumes a converged cluster. A queued ADR-047 partial apply can temporarily leave an old
body live at its prior owner and a replacement live at a different owner, making both emissions
individually ownership-valid. The coordinator therefore checks `pending_repairs` before delivery
and at every shard boundary. A nonzero value fails the job before completion (before any chunk in
the pre-existing case); the operator must run `resync` (or reopen a durable in-process coordinator,
whose log replay reconstructs the repair) and retry. Exact cross-shard deduplication is
intentionally not added because it would require result-sized state.
A fresh in-memory coordinator attached to already-populated remote shards is a separate
unattested state: its empty `pending_repairs` map says only that this process witnessed no failure,
not that a prior coordinator left no partially-applied cross-owner mutation. `from_parts` already
marks the logical-id directory unauthoritative in this shape because the wire cannot enumerate the
live corpus. Exhaustive delivery uses the same authority bit as a convergence precondition and
refuses before any chunk. An initial multi-shard `ingest_with_tags` failure revokes that authority
too: a lower bucket may already be installed, and the failing remote call itself is ambiguous, but
bulk load has no per-logical ADR-047 repair record. Retaining all id reservations prevents a
conflicting incremental add, while the revoked attestation prevents an empty repair map from
certifying a partial corpus. `resync` cannot reconstruct either unknown history. Exact delivery
resumes only after fresh shard slots are rebuilt from the authoritative corpus (or a future durable
remote coordinator/live-id enumeration or bulk repair journal supplies the missing attestation).
Process-local convergence state is also insufficient when two fresh coordinators attach to the
same *empty* remote shard set: both would otherwise see zero rows and independently declare their
own mutation barrier authoritative. Exact-capable remote assembly therefore uses
`connect_remote_exclusive` / `connect_replicated_exclusive` with one caller-stable non-zero
identity, stamped on all shard RPCs. (The HTTP cluster connector selects this mode.) A shard node
atomically claims that identity only through a valid
`DictFingerprint`/`AdoptDict`/`AddShard` handshake; the one-shot claim capability is rejected on
every other RPC. A transition first stops new unstamped or prior-owner admission and drains
already-admitted calls through their complete response body (including stream EOF or disconnect),
then publishes the owner. Cancelling a waiting handshake rolls its claim registration back unless
another same-identity waiter remains. The server echoes the owner in handshake and fingerprint
attestations, and thereafter rejects unstamped or differently stamped traffic before any handler
runs. All slots on a co-located node share the lease. Thus another
coordinator cannot mutate, recover, or exhaustively read any member of the first coordinator's
shard set; a partial multi-node claim fails closed rather than creating two usable coordinators.
The historical `connect_remote` / `connect_replicated` builders remain unleased for compatibility
with existing multi-coordinator recovery and restart workflows, but their exhaustive API refuses
before emitting a chunk because they cannot attest this cross-process barrier. An exclusive claim
also fences those compatibility clients from that node. A pre-lease shard binary echoes protobuf
zero and is refused by an exclusive builder, so a mixed-version mesh cannot silently bypass the
fence. The lease is renewable with a 30-second bound. Owner-stamped RPCs renew it; a different
identity is rejected before expiry, while an explicit post-expiry claim drains every active
prior-owner/legacy body before publishing. This lets a stateless coordinator restart eventually
replace a dead boot ID instead of wedging permanently. A durable shard-process restart clears the
lease, so an existing client uses a claim-stamped, read-only fingerprint handshake to verify the
restored node and retry once without creating a slot. Remote coordinator restart still lacks
durable convergence authority, so exact delivery requires fresh shard slots rebuilt from the
authoritative corpus as described above.
Even with no queued repair, a successful add/upsert/remove could otherwise move a row between two
sequential shard reads. The complete fan-out therefore takes the exclusive side of the existing
coordinator mutation/PIT-open barrier; live mutations and `resync` hold the shared side. The
library API and HTTP job surface consequently see one coherent logical view, while a deadline or
sink cancellation can still break the wait to acquire that barrier. Whenever the mutation also
needs a logical-id stripe, the global lock order is barrier then stripe on every live-write,
bulk-load, and repair path; this avoids a queued exclusive reader forming a writer-preference
cycle between a same-id mutation and `resync`.
Any required shard failure fails the whole job after any already-sent chunks; there is no partial
success or broad-scope downgrade.

### Serving admission and backpressure are separate from interactive search

The server owns a dedicated exhaustive Rayon pool and a separate bounded semaphore. Admission is
non-queuing at the HTTP front door: exhausted node capacity returns 503; a future tenant-aware
outer quota may return 429. Each shard node also owns an independent non-queuing `PercolateAll`
semaphore and acquires it before `spawn_blocking`, because direct clients or multiple coordinators
can bypass the HTTP quota; excess shard streams fail with gRPC `RESOURCE_EXHAUSTED`. A shard also
rejects a caller-supplied remaining budget above its server-owned
`--max-exhaustive-stream-secs` ceiling (default 300 seconds). A direct client can therefore neither
bypass the coordinator's HTTP timeout nor retain every node permit for an arbitrarily long stalled
stream. The node concurrency and duration ceilings remain independently configurable from the HTTP
quota because direct gRPC callers can reach the shard process even though a claimed remote node
admits only its one exclusive coordinator identity. Chunk size, channel depth, job timeout,
concurrency, and retained terminal-job count are explicit bounded settings. The one absolute job
deadline is armed when the execution permit is admitted, before registry work or worker scheduling,
so dedicated-pool queue time is part of the maximum lifetime. The execution permit
is claimed before a terminal record is pruned for retention capacity, so a rejected/busy request
cannot destroy a status, event-id mapping, or unclaimed stream without admitting its replacement.

The retained `event_id` index compares a fingerprint of the effective request after defaults are
applied. It hashes a canonical raw key/value predicate and last-write-wins boost map rather than
resolved `TagId`s: omitted versus explicit `standard` scope/default priority/default timeout,
reordered filter values or effective boosts, and the two accepted stream-sink spellings therefore
reuse one job, and an ordinary standalone write that interns a previously unknown tag cannot turn
an identical retry into `409 event_id_conflict`. Distinct boost pairs that resolve to the same
synthetic `TagId` are rejected as ambiguous before admission; repeats of one raw pair retain their
documented last-write-wins meaning.

The sync collector publishes into a bounded Tokio MPSC channel from the exhaustive worker. It
uses bounded retry/polling so backpressure time is measured and cancellation/deadline checks can
break a blocked send. The cluster HTTP job also polls while waiting for its serving-layer write
mutex. Cancellation observed while the terminal completion frame is waiting for channel capacity
records the job as `cancelled`, never as a generic delivery failure. After enqueue, the same
bounded wait continues until the HTTP stream dequeues the terminal frame; cancellation, deadline,
or receiver drop invalidates the queued completion and prevents `completed` status. Those
invalidation checks and terminal dequeue use the same terminal-state gate, eliminating an
observation-to-invalidation race in which both cancellation and completion could otherwise appear
to win. A queued shard closure retains its permit until Tokio actually schedules it, while its
sole response sender remains revocable by an async deadline/disconnect watcher. This deliberately
keeps an expired dormant closure charged against the configured concurrency bound, preventing
permit recycling from enqueueing an unbounded number of replacements behind unrelated global
blocking work. The closure sends an explicit start signal; that signal makes the watcher drop its
extra sender immediately, so a successful summary is followed by EOF rather than an open channel
retained until the deadline.
The shard gRPC sink uses the request's absolute deadline to bound every full-channel wait;
the remote client polls its downstream sink while awaiting headers/frames and drops the stream on
cancellation, so the server observes a closed receiver. Axum's streaming body and tonic's HTTP/2
stream propagate downstream demand; every shard-side wait is bounded by an accepted request budget
that cannot exceed the node ceiling. Metrics cover chunks, encoded bytes, backpressure seconds, and
jobs by terminal state. Remote polling constructs Tokio timers only after entering the stored
runtime handle; the ordinary caller is a plain exhaustive Rayon worker, where eagerly constructing
`tokio::time::timeout` would panic.
`HEAD` on the single-consumer stream is rejected before the receiver is claimed. Shutdown requests
cooperative cancellation of every running job before taking engine/coordinator write locks, so an
unclaimed, backpressured stream cannot extend cleanup to the job's much longer timeout.

Broker durability remains outside the core. The server feature includes a small reference
`BrokerPublisher` adapter that retries a keyed frame without changing its idempotency key; Kafka,
Pub/Sub, SQS, JetStream, or another operator-selected implementation supplies the actual
publisher and retention policy.

## Consequences

- Exhaustive result memory is `O(chunk_size)`, independent of true-match count. Result bytes can
  be arbitrarily large without one giant response allocation.
- Fault injection at every chunk boundary has one of two outcomes: a verifiable terminal
  completion, or a failed job with no completion. Already-delivered provisional chunks may repeat
  on retry but cannot be mistaken for a committed truncated set.
- Cancellation, sink failure, and gRPC backpressure are observed independently of chunk emission;
  they stop candidate/posting work and release dedicated capacity within bounded polling latency.
- The default compatibility and ranked paths are unchanged. No persistence-format change is
  required; the gRPC addition is additive and stale peers fail loud when exhaustive delivery is
  requested.
- Jobs are in-memory orchestration. Restart loses job state and open streams; durable replay and
  consumer groups belong to the configured external broker. The boot-random generation namespace
  prevents a restarted process from reusing the lost job's durable member keys.
- No global member ordering is promised. Clients that require sorted traversal must materialize
  externally after terminal completion.
- Snapshot generation is an opaque boot-namespaced identifier for the captured job execution
  view/placement. It is an idempotency boundary, not an ordering promise or a substitute for
  ADR-113 cursor pagination.
- Exact competitive pruning was evaluated separately and declined for now because its profiling
  prerequisite did not fire (ADR-115); this increment addresses the evidenced
  emission/delivery boundary.
