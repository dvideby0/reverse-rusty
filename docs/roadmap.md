# Roadmap

This is the canonical home for **unfinished work only**, ordered by expected leverage. Each item
contains enough context to evaluate and start the work without opening a separate proposal. Research,
ADRs, and design docs are linked where they provide evidence or constraints.

When an item ships:

1. record the decision in an ADR when architecture or compatibility is affected;
2. add the shipped outcome to [CHANGELOG.md](CHANGELOG.md);
3. remove the item from this file rather than marking it complete.

Current behavior belongs in [design](design/README.md), [reference](reference/api.md), and
[operations](operations/deployment-modes.md). Decision history belongs in
[the ADR hub](DECISIONS.md).

## Priority 1 — real-world acceptance evidence

### Real-corpus correctness and throughput audit

**Problem.** The engine has extensive synthetic, adversarial, independent-oracle, durability, and
20M-query scale coverage, but the final distributed-v1 credibility gap is still a representative
production corpus. Synthetic data proves invariants; it cannot establish the real distribution of
query shapes, aliases, broad work, duplicate bodies, memory use, or throughput.

**Direction.** Use the `RR_ORACLE_CORPUS` intake from
[ADR-087](decisions/adr-087-independent-correctness-oracle.md) to run one reproducible audit:

- translate stored queries into the documented DSL and retain rejected-query reasons;
- compare the engine with the independent matcher for exact semantics and candidate recall;
- capture title throughput, candidate volume, lane work, shard fan-out, duplicate-body rate,
  memory, durable bytes, and reopen time;
- publish only aggregate evidence and a reproducible harness, not private corpus data.

**Completion.** Zero candidate false negatives and zero final-set mismatches on the accepted corpus;
an explained disposition for every rejected or unsupported query; and a dated performance capture in
[`performance/`](performance/). The same evidence decides whether the two measurement-gated
broad-query items below are worth their format and complexity costs.

### Real Kubernetes failure and recovery exercise

**Problem.** Compose, localhost gRPC, crash injection, and the Helm smoke prove the packaged
mechanics, but not the behavior of a real scheduler, persistent volumes, network policy, secret
rotation, and node failure.

**Direction.** Deploy the supported Helm topology from
[`operations/deployment-modes.md`](operations/deployment-modes.md) to a real cluster, ingest the
real-corpus audit set, and execute a documented fault matrix:

- restart every process role and roll every image;
- delete a shard pod and its node, then recover it according to the configured RF;
- exhaust a data volume and verify writes fail closed;
- rotate mesh and HTTP secrets;
- take, verify, restore, and query a backup;
- compare every successful probe with the pre-fault reference result set.

**Completion.** No silent misses, a measured RPO/RTO for each scenario, retained logs and metrics,
and runbook corrections for every operator decision discovered during the exercise.

## Priority 2 — query cost and memory at scale

The shipped hot tier and in-memory body sharing are described by
[ADR-105](decisions/adr-105-hot-tier-two-axis-placement.md) and
[ADR-106](decisions/adr-106-canonical-body-dedup-stage-a.md). Any follow-up must preserve the
two-axis placement rule: **cost movement must never change visibility**.

The broad-cost program also established boundaries that still apply to every item here:
do not suppress correct member IDs to reduce output volume; do not add N-wide mutable counters to
the title hot path; keep reorganization at immutable-segment build or compaction seams; and do not
revive score-bound early termination without new profiling evidence that clears
[ADR-115](decisions/adr-115-competitive-pruning-deferred.md).

### Persist canonical-body indirection on disk

**Problem.** Stage A shares one posting entry for identical semantic bodies inside memory segments,
but flush expands every member into the mmap format. A durable corpus therefore loses the sharing
where most queries live.

**Evidence gate.** Do not add a format indirection from synthetic results alone. First use the
Stage-A `distinct_bodies_est`, `bodies_total`, and `dup_joined` telemetry on the real corpus. Proceed
only if the durable-byte and posting-scan reduction is material after accounting for member lists,
tags, priorities, and versions.

**Direction.**

- add a versioned body record with one exact-verification row and a list of logical members;
- preserve member-level aliveness, tags, priority, version, source, delete, and upsert semantics;
- confirm semantic-body hash matches with exact equality so a collision can never false-share;
- teach mmap reads, compaction, recovery, backup, metrics, and explain about the indirection;
- keep legacy segments readable or fail loudly behind an explicit migration fence.

**Completion.** Differential equality with sharing on and off across live writes, flush, compaction,
reopen, WAL recovery, tags, ranking, and tombstoned leaders; measured durable and scan reduction on
the real corpus; and a rollback-safe format ADR.

### Pair-anchor escalation and residual factoring

**Problem.** A multi-feature query can remain expensive when each feature is individually hot but
their conjunction is selective. Single-feature frequency cannot identify that shape, and verifying
every full body repeats work already proven by the posting probe.

**Evidence gate.** Use the real-corpus audit to show that these queries contribute material
postings or verification cost after the hot tier and body sharing. If they do not, leave the matcher
simple.

**Direction.**

- collect bounded joint-frequency evidence only for query-nominated feature pairs;
- escalate qualifying queries to pair anchors when the conjunction is selective;
- persist the classification evidence needed to reproduce the decision;
- subtract probe-implied required features and group the remaining verification work by residual
  shape for size-specialized kernels.

**Correctness constraints.**

- The compiler's pair-selection predicate and the title-side pair-generation predicate are an
  **agreement fence** and must change together.
- Pair escalation may change evaluation cost, never default or broad visibility.
- Negative features remain unavailable to anchor selection.
- Migration must be oracle-gated across old/new segments, compaction, replay, and cluster routing.

**Completion.** A format and compatibility ADR, mutation-validated differential coverage for the
agreement fence, and a real-corpus reduction in scanned postings or verifier work.

### Dense representation promotion in the batch evaluator

**Problem.** The broad and hot columnar passes can still spend time iterating postings that are
dense relative to the active candidate set. Storage already promotes large lists to roaring
bitmaps, but the batch evaluator does not independently choose the cheapest counting
representation.

**Direction.** Profile the real corpus after the hot tier and count gate, then select list,
bitmap, or dense scratch representation from measured relative density. The choice is internal to
evaluation and must not affect the candidate set. Treat the previously suggested 40% switch point
as prior art, not a default.

**Completion.** Scalar/columnar result equality, a stable density threshold across repeated
captures, bounded scratch memory, and a demonstrated broad or hot batch improvement.

### Tag-aware segment skipping

**Problem.** Metadata filtering is the dominant percolator read pattern, but the engine still opens
every candidate-bearing segment even when the request predicate cannot accept any tag row in that
segment.

**Direction.** Add immutable per-segment tag summaries or partitions that can prove a segment has no
acceptable row. The optimization must be request-filter-driven and fail open: an absent, stale, or
inconclusive summary probes the segment normally. Tags remain outside semantic signature generation,
and negative query features remain unavailable.

**Completion.** Filtered result equality with skipping enabled and disabled across writes,
compaction, recovery, synthetic tag IDs, and cluster fan-out; a format-compatible persistence plan;
and representative evidence that avoided segment work exceeds summary lookup cost.

### Memory headroom for 100M-query deployments

**Problem.** The shipped durable `retain_source=false` profile already leaves canonical source text
in the mmap-backed source store and reads it lazily, reducing engine-accounted resident memory from
roughly a little over 100 B/query to about 5–6 B/query in the current captures. Those measurements
do not establish host RSS, source-read working set, page-cache pressure, or memory-bandwidth behavior
at 100M queries, and the remaining dictionary, index, and verification columns may become the
dominant resident cost.

**Direction.** Measure the components separately, then address the parts that are material:

- measure and operationalize the existing `retain_source=false` path under realistic
  source/explain reads, including RSS, page-cache churn, and I/O latency, and decide whether it
  should become the recommended large-corpus profile;
- reduce source-store indexes or add a bounded source cache only if those measurements identify
  source lookup as material;
- pool immutable dictionary string bytes where allocation overhead is measurable;
- tighten SoA fields and access order to reduce bytes touched per candidate.

Potential techniques include pooled string storage, mmap-backed immutable dictionaries, and narrower
columns where format bounds prove them safe. Aliveness is already bit-packed; do not count that as
future savings. SIMD is useful only after a profile identifies a stable intersection kernel.

**Completion.** A measured 100M memory model with component attribution, no regression to the
allocation-free matching contract, and pressure tests showing that the new representation remains
lossless through persistence and reopen.

## Priority 3 — distributed lifecycle and ranked-path efficiency

### Automatic and remote cluster resize

**Problem.** In-process resize is a manual blue/green rebuild. The autoscaler can recommend a split
but cannot safely execute repeated resizes, and a remote cluster cannot re-key data online.

**Direction.**

1. Add hysteresis, cooldown, idempotency keys, progress state, and abort recovery around the
   existing in-process resize.
2. Generalize the operation to a remote blue/green topology: create target slots, stream the
   re-placed corpus, validate fingerprints and query counts, atomically switch committed routing,
   then garbage-collect the old layout.
3. If measured rebuild cost justifies the additional state machine, add a targeted online split:
   build shadow children, drain the mutation tail, prove their fingerprints, and switch the affected
   ring range atomically. Do not introduce dual routing as an unmeasured prerequisite.

The controller must distinguish a recommendation from an accepted operation; corpus-size noise must
not create resize thrash.

**Completion.** Repeated grow/shrink operations converge under concurrent writes and injected
failure, restart resumes or safely aborts an operation, and every acknowledged query remains
matchable before and after the routing cutover.

### Staged replica recovery outside the fence window

**Problem.** A genuinely stale retained member is rebuilt while the replicated group is fenced.
Fingerprint reuse collapses the common no-copy case, but a large divergent member still makes the
write pause proportional to corpus copy time.

**Direction.** Recover into a shadow slot before fencing, track its translog catch-up point, then
fence only for the bounded final drain and atomic promotion. The live slot must remain untouched
until the shadow proves complete.

**Completion.** Recovery time outside the fence is unbounded but the fenced interval is bounded by
tail catch-up; crash injection at every phase leaves either the old complete slot or the new complete
slot routable, never a partial install.

### Seed the remote logical-ID directory

**Problem.** A fresh coordinator attached to populated remote shards cannot enumerate existing
logical IDs. Its admission directory is therefore unauthoritative and must conservatively treat an
add as an upsert.

**Direction.** Add a bounded `LiveLogicalIds` shard RPC, delegate it through handoff and replica
composites, collect/sort/deduplicate the IDs at connect time, and install the directory atomically.
Enumeration failure must leave the current fail-closed behavior in place.

**Completion.** A reattached coordinator rejects create-only writes for existing IDs, accepts new
IDs, and reconstructs the same directory across primary failover and restart.

### Replace striped cluster write locks with per-ID locks

**Problem.** The fixed stripe table serializes unrelated logical IDs that hash to the same stripe.
The lock must still cover WAL append and the complete shard fan-out because same-ID operations may
not interleave.

**Direction.** Use a bounded lifecycle-managed lock table keyed by logical ID. Preserve the full
same-ID critical section and the whole-directory exclusion used by bulk load; reclaim idle entries
without allowing two locks for the same active ID.

**Completion.** Same-ID live ordering remains replay-equivalent under failure, unrelated IDs no
longer serialize on hash collision, and the table stays bounded under high-cardinality churn.

### Ranked-path allocation and merge cleanup

**Problem.** The bounded ranked path still allocates during ownership validation, repeatedly scans
rank metadata, fully sorts already-sorted shard runs, and clones request groups during fetch.

**Direction.**

- validate borrowed placement views instead of allocating one vector per row;
- cache newest-live rank metadata inside the collector and reuse pooled scratch;
- perform a bounded S-way merge over sorted shard runs;
- remove per-shard O(K) clones from winner fetch.

**Completion.** Preserve exact ordering, totals, ownership, and winner-source behavior while
`rankbench` demonstrates lower allocation and coordinator CPU at fixed K.

## Priority 4 — feature-model evolution and parity

### Versioned feature models with blue/green re-materialization

**Problem.** Minor runtime vocabulary changes are supported, but a major tokenizer, feature-kind,
or common-mask change cannot safely reinterpret rows compiled under an older model.

**Direction.** Give the complete feature model a durable version and fingerprint. Keep compatible
minor changes within the existing epoch machinery; rebuild major changes into a parallel index from
canonical sources, validate it against the independent oracle, then atomically swap the serving
epoch.

**Completion.** Mixed model versions fail loudly, rollback retains the previous complete index, and
the blue/green swap is result-equivalent across crash and reopen.

### Self-tuning cost and placement recommendations

**Problem.** The engine exposes raw lane and candidate telemetry but does not turn it into stable,
actionable recommendations.

**Direction.** Add offline or background analysis for:

- candidate-survival rates by anchor and query shape;
- recommendations beyond the existing corpus-size shard-count helper, including measured fan-out,
  posting cost, and anchor arity;
- feature-ID re-ranking for locality during a model rebuild;
- corpus learner reruns per range when data distributions diverge.

Recommendations must be inspectable and opt-in. They may suggest a rebuild or compaction policy but
must not silently change query visibility.

**Completion.** Recommendations reproduce from a pinned capture, explain their evidence, remain
stable under small workload perturbations, and have an explicit apply/rollback workflow.

### Vocabulary consolidation during compaction

**Problem.** Feature hashing and learned aliases keep post-freeze vocabulary lossless, but a mature
corpus can accumulate synthetic IDs and stale vocabulary generations.

**Direction.** During a controlled model rebuild, promote sufficiently stable hashed terms and
reviewed aliases into a new dense dictionary, then recompile affected rows. This is distinct from
re-anchoring: it changes the feature space and therefore requires a new model fingerprint.

**Completion.** No false negatives across old/new generations, deterministic ID assignment,
bounded migration memory, and measured benefit in dictionary locality or collision rate.

### Aspects-first ingestion

**Problem.** Marketplace item specifics often carry cleaner brand, model, size, and condition
signals than title parsing, but the current feature model is title-centric.

**Direction.** Define typed structured fields as additional positive and negative features, with
one shared query/document normalization contract and explicit precedence when structured and title
signals disagree. Keep source-specific strings out of the match hot path.

**Completion.** A documented DSL/API mapping, zero-false-negative oracle coverage across title-only
and aspects-aware modes, and real-corpus evidence that aspects reduce broad work or ambiguity.

### Alias and punctuation recall refinements

**Problem.** Several shipped seams still have optional recall, feature-quality, or performance
refinements:

- preserve the scattered-component reading when a multi-word alias activates;
- emit both joined and split forms for selected punctuation folds;
- propose high-confidence edit-distance-one aliases for rare misspellings;
- type common card-number forms such as `#866` and `#BDC-85` as one selective feature;
- keep multi-word aliases on the columnar broad path instead of falling back to scalar evaluation.

**Direction.** Treat alias, punctuation, and typo changes as bounded additive expansions; keep typo
activation review-first. Card-number typing must use the same query/title rule and preserve the
generic form during migration. The columnar change is evaluation-only. Every knob defaults to
current behavior.

**Completion.** Independent-oracle and forbidden-feature matrices prove no false negatives; the
columnar path is exactly equal to scalar matching; real examples justify the additional candidates.

## Priority 5 — service automation and security

### Backup and restore as a cluster service

**Problem.** Backup is engine-driven but still an operator procedure. A remote cluster lacks one
API-owned consistency barrier, schedules, retention, and continuous restore verification.

**Direction.** Add coordinator-owned backup and restore jobs with:

- a cross-shard consistency point and manifest;
- per-shard checksums and resumable progress;
- schedules, retention, and restore verification;
- explicit RPO/RTO and cancellation semantics.

Export to object storage requires an ADR that narrows or amends the shared-nothing decision in
[ADR-033](decisions/adr-033-shared-nothing-storage.md); do not introduce it implicitly.

**Completion.** A killed coordinator can resume or safely abandon a job, a restored cluster proves
the same logical corpus, and operators no longer coordinate shard snapshots manually.

### Kubernetes operator and RF>1 topology

**Problem.** Helm installs static resources but does not own lifecycle state, backup schedules,
blue/green resize, or replicated placement.

**Direction.** Introduce a `ReverseRustyCluster` custom resource and controller only after the
underlying APIs are stable. The operator should reconcile StatefulSets, placement, backups,
restores, rollouts, and resize state. Extend the chart to express RF>1 without requiring operators
to hand-build per-position replica groups.

**Completion.** Reconciliation is idempotent across controller restart, status exposes actionable
conditions, and end-to-end tests cover upgrade, backup, restore, resize, and whole-node loss.

### Security hardening beyond the v1 trust model

**Problem.** The current mesh uses server-authenticated TLS plus one shared token. Backup destinations
are trusted operator input, and the runtime image favors operability over minimum attack surface.

**Direction.** Evaluate mTLS node identity, per-RPC authorization, a configured backup destination
jail, secret rotation without restart, and a smaller runtime image. Preserve the current simple
trusted-network mode as an explicit deployment profile.

**Completion.** The threat model and deployment contract define both profiles, credential rotation
is exercised in the real-cluster drill, and security controls fail closed without breaking health
or recovery traffic.

## Later improvements

These are valid but lower-leverage than the items above. Each entry states the problem, intended
change, and acceptance boundary; promotion changes its priority, not its documentation home.

### API and operator ergonomics

- **CORS policy.** Browser tools cannot call the API across origins today. Add an explicit
  configurable `CorsLayer`, default it to no cross-origin access, document credential handling, and
  test preflight behavior with authentication enabled.
- **Thread-pool introspection.** Search admission is bounded but operators cannot see queue
  pressure directly. Expose active, queued, rejected, and completed work through fixed-cardinality
  metrics and an operator endpoint, then verify the counters under saturation.
- **Segment filter quality.** `/_cat/segments` reports filter bytes but not whether the bloom
  allocation is effective. Retain inserted-key and block counts, expose an estimated false-positive
  rate, and compare it with a sampled measured rate.
- **Complete `_cat` controls.** Add `?v`, `?h`, and `?help` consistently across catalog endpoints,
  with one shared column-selection parser and typed errors for unknown fields.
- **Batch cursor pagination.** `/v2/_mpercolate` intentionally rejects PIT and cursor state today.
  Add per-title continuation only if a real workload needs it, with bounded aggregate cursor state
  and the same snapshot, ordering, and stale-cursor guarantees as `/v2/_search`.
- **Read-auth policy consistency.** With `--auth-protect-reads=false`, the compatibility search
  POSTs, `/v2/_search`, PIT lifecycle, and exhaustive-job creation are treated as reads, while
  `POST /v2/_mpercolate` still requires the bearer token. Decide whether the v2 batch surface is a
  protected operation or a read, then align the allowlist, reference, and both auth-mode tests.
- **Stable duration formatting.** Raw floating-point `took_ms` values expose serialization noise.
  Define bounded precision or an integer duration unit and pin the response contract.
- **Cold-start prewarming.** An mmap reopen can shift first-query latency into page faults. Add an
  opt-in byte/time-budgeted page-touch strategy and retain it only if measurements show useful
  latency reduction without uncontrolled resident growth.
- **Restart measurements.** The design extrapolates reopen behavior but the operator guide needs
  evidence. Capture 1M, 20M, and real-corpus reopen times, add the dated results to
  [`performance/`](performance/), and update sizing guidance from those captures.
- **Broad-query ingest guidance.** Operators can observe an expensive lane only after cost
  accumulates. Report the assigned lane at ingest and suggest a selective rewrite when semantics
  permit, but never reject or silently rewrite a query.
- **Opaque original-expression passthrough.** Drop-in users may compile a deliberately widened RR
  translation but need their foreign precision matcher to re-read the untouched original expression.
  Add an optional opaque source field that is persisted and returned without participating in RR
  parsing, matching, ranking, or routing; keep the translated RR query as the compilation source.

### Local memory and hot-path cleanup

- **Reusable WAL encoding.** Pool serialization buffers across writes while preserving frame
  atomicity and ensuring a failed append cannot leak bytes into the next frame.
- **Faster manifest CRC.** Replace byte-at-a-time CRC with a table or hardware-assisted path,
  retaining byte-identical checksums and malformed-manifest failures.
- **Profile-gated SIMD.** Evaluate vectorized intersections only when representative profiles are
  dominated by medium or large postings; keep the scalar path when setup cost wins.

Each change must retain the allocation-free unarmed match path and include a before/after workload
capture; microbenchmarks alone are not sufficient.

### Test infrastructure

- **Phrase-pattern fuzzing.** Expand the parse-union alphabet with punctuation markers, number
  context, years, and fused graders after teaching the independent reference emitter the same
  documented surface grammar.
- **Cross-seam matrix.** Combine recovery, vocabulary adoption, rebuild, and remote attach in one
  bounded matrix because point tests do not catch ordering failures between those seams.
- **Targeted mutation testing.** Run mutation testing on normalization, compile, and exact-match
  modules after major semantic changes; keep it out of the per-PR gate unless runtime becomes
  predictable.
- **Messy cluster oracles.** Thread deterministic messy-data generation through cluster and
  durability oracles and require equality with their clean-data reference results.

### Code and error-surface cleanup

- **Remove legacy shard reads.** Delete the dead non-ownership-aware read methods after every test
  and implementation uses the owned collector path; public result semantics must remain identical.
- **Reuse storage readers.** Expose the existing checked little-endian helpers within the crate and
  remove the adopted-space decoder's duplicate truncated-read logic.
- **Precise durability labels.** Emit segment-write or segment-mmap operations at the failing
  durable-ingest step instead of the broader ingest-rollback label, while preserving the original
  typed error and rollback behavior.
