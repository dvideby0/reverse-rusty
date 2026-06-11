# ADR-076: Multi-word aliases on a cluster (P(T)-aware routing) + the vocab-shipping decision

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted (2026-06-11). Closes [ADR-065](adr-065-distributed-v1-graduation.md)
  criterion **6** — the ADR-046/061 deferrals — with one half **shipped** and one half
  **decided as a documented refusal** (the criterion demanded "decided, not
  deferred-by-default").
- **Context:** Two deferrals lived here. **(a)** A vocabulary change is a normalizer
  operation; remote shards normalize titles themselves, so a live `set_vocab` would need to
  *ship* the new normalizer cross-process or a stale shard silently misses matches —
  `set_vocab` refuses non-local clusters (ADR-046). **(b)** Multi-word aliases (ADR-061)
  were single-node only: cluster routing derived a title's target shards from the canonical
  leftmost-longest view `N(T)`, so a nested alias entity living only in the positive
  superset `P(T)` never probed the shard holding a query anchored on it — a false negative
  routing could not recover, enforced by loud refusals at every cluster constructor +
  `set_vocab`.
- **Decision (b) — SHIPPED: P(T)-aware routing.** `route` now derives targets from
  `match_features_dual`'s **maximal positive view** when the normalizer has active
  multi-word aliases. The cover argument: a query's anchor is one of its extracted positive
  features, and `P(T)` contains every feature ANY parse of the title emits (the parse-union
  property the ADR-061 oracle pins) — so a title that could satisfy a query always routes
  to the query's anchor shard, zero false negatives. `P(T) ⊇ N(T)` ⇒ fan-out only ever
  widens, and only on alias-bearing titles; with no active multi-word alias `P(T) == N(T)`
  and the single-view path runs — **byte-identical routing for every existing cluster**.
  The shard-local two-view verifier (ADR-061) was already correct once the probe arrives.
  All three refusals (build, `from_parts`, `set_vocab`) are retired.
- **Found while flipping the build refusal — the bare-normalizer activation gap (fixed):**
  a cluster built from a bare `Normalizer` never installs the vocab's **equivalence
  machinery** on the minted dict, so declared equivalences and registry aliases (ADR-054/060
  — single-token included) were silently **inert**: queries didn't expand, and the
  coordinator-mode server's `--vocab` startup path dropped the loaded `Vocab` after deriving
  the normalizer (pre-ADR-076 a *multi-word* file at least failed loudly on the refusal;
  single-token registry aliases sailed through inert). Identical to single-node
  bare-normalizer semantics — but the server path made it an operator trap. Fixed:
  - **`ClusterEngine::build_with_vocab`** — the activating constructor, mirroring
    `Engine::with_vocab`'s fresh-path order: self-heal (`demote_unexpressible`), intern the
    active equivalence/alias forms into the fresh dict (pinning dense ids), install the
    resolved equivalence map **before** the corpus extracts (queries expand; placement fans
    widened any-ofs), install the vocab on the engine, and **persist it in the durable
    manifest from the first commit** (a crash before any later checkpoint still reopens
    with the vocabulary in effect).
  - The coordinator-mode server's fresh in-process build routes `--vocab` through it; a
    reopen keeps the manifest's persisted vocab authoritative.
  - Building from a bare normalizer remains accepted with single-node-parity semantics
    (pinned by an oracle test) — the boundary is documented, not silent divergence.
- **Decision (a) — REFUSED, documented: no live cross-process vocabulary shipping at v1.**
  Remote-cluster vocabulary is **deploy-time configuration**. Prior art is decisive: ES
  cannot change an analyzer on a live index — you reindex into a new index and swap an
  alias. Reverse Rusty's in-process cluster automates exactly that rebuild (`set_vocab`'s
  blue/green re-mint + re-place); a remote cluster does it at the deployment level: update
  the vocab file, redeploy shard nodes + coordinator, reload (blue/green at the cluster
  level — the criterion-10 runbook documents the procedure). Supporting reasons:
  - A correct live remote rebuild is cluster-wide blue/green over gRPC: gather sources from
    remote shards (no RPC exists), re-place + re-ingest the corpus over the wire, ship the
    re-minted dict + normalizer to every node, and flip atomically against a propagation
    window — handoff-grade machinery per vocabulary change.
  - It collides with the ADR-074 boundary: a tagged remote rebuild would need synthetic
    `TagId`s on the wire, which is only sound behind the criterion-9 tag-dict fingerprint
    handshake (not yet built).
  - The **mesh refuses loudly today**: `set_vocab` keeps its non-local refusal, and the
    coordinator-mode server now **fails startup** when a remote assembly is given a vocab
    file carrying equivalence-driven rules (they would be silently inert; plain
    synonyms/phrases/punctuation are normalizer-level and ship fine via the out-of-band
    `norm`). Nothing degrades silently.
- **Why this is safe:** routing only ever widens (a superset of probed shards can only add
  candidates, never lose one — and the exact verifier rejects false positives); alias-free
  clusters are byte-identical on every path; `build_with_vocab` is a new constructor
  (existing constructors unchanged); the startup refusal turns an existing silent failure
  loud. The lossless-cover contract extends to the cluster: title-side `P(T)` retrieval ∘
  P(T)-aware routing ⊇ any single parse's features.
- **Proven:** `tests/cluster_oracle/vocab_learning.rs` — `set_vocab` activates a multi-word
  alias K∈{1,3,8} (both surface forms match; cluster ≡ a single-node engine under the same
  vocab on alias titles AND a corpus sample); **the constructible pre-fix false negative**
  (overlapping aliases: a nested alias entity exists only in `P(T)`, and the inner-alias
  query places SELECTIVELY on shards the title's canonical view never routes to — matched
  now at every K, with the selective placement + the `P(T) ⊋ N(T)` preconditions pinned so
  the construction can't silently stop exercising the fix); `build_with_vocab` activates at
  construction AND persists the vocab from the first durable commit (reopen with a bare
  normalizer still matches both forms — no intervening checkpoint),
  the multi-word alias **survives durable checkpoint + reopen** (the persisted-manifest
  vocab drives P(T)-aware routing from disk alone), the bare-normalizer boundary is pinned
  (cluster ≡ single-node inert semantics), and the unexpressible-alias self-heal still
  demotes at the install seam. Full default + `distributed` suites green.
- **See also:** ADR-061 (the dual-view mechanism + the single-node-only decision this
  lifts), ADR-046 (dynamic vocabulary; mechanism 1 was always cross-process-free), ADR-054/
  060 (the equivalence machinery `build_with_vocab` installs), ADR-074 (tags through the
  rebuild; the wire boundary that shapes decision (a)), ADR-065 (the program; criterion 9
  is the tag-dict handshake, criterion 10 owns the redeploy runbook),
  [`research/dynamic-vocabulary.md`](../research/dynamic-vocabulary.md) §6 (the spike that
  scoped the shipping design this ADR declines for v1).
