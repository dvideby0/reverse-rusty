# ADR-120: Quoted phrases use analyzed token-graph adjacency

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** The DSL documented `"a b"` and `-"a b"` as phrase predicates, but extraction lowered
  their normalized features into the same unordered required/forbidden columns as bare terms.
  `"red shoe"` therefore matched `red leather shoe`, while `-"for parts"` rejected
  `for spare parts`. That contradicted the language reference and the usual ES/OpenSearch meaning of
  a phrase. The shared-front-end and independent matchers had copied the same underspecified
  lowering, so their differential could not expose the error.

- **Prior art.** Elasticsearch and OpenSearch `match_phrase` analyze the input and require terms at
  consecutive positions when `slop` is zero
  ([Elasticsearch](https://www.elastic.co/guide/en/elasticsearch/reference/current/query-dsl-match-query-phrase.html),
  [OpenSearch](https://docs.opensearch.org/latest/query-dsl/full-text/match-phrase/)).
  Lucene represents analysis alternatives with position increments and graph edges rather than
  flattening them into a bag
  ([Lucene position attributes](https://lucene.apache.org/core/10_3_1/core/org/apache/lucene/analysis/tokenattributes/PositionIncrementAttribute.html),
  [Elasticsearch token graphs](https://www.elastic.co/guide/en/elasticsearch/reference/current/token-graphs.html)).
  Reverse Rusty adopts that positional contract while retaining its existing integer feature
  analyzer and two-view alias policy.

- **Decision — exact analyzed adjacency.** A quoted clause matches when its complete analyzed query
  graph occurs as one connected path through the analyzed title graph, in order, with no unmatched
  title position between successive edges. This is zero-slop adjacency over normalized tokens, not
  raw-byte equality:
  - default split punctuation makes `"red shoe"` match `red-shoe`;
  - configuring `-` as `Fold` makes `"red-shoe"` one `redshoe` token instead;
  - case, diacritics, synonyms, grader/number typing, phrase entities, and punctuation use the same
    normalizer on both sides;
  - analyzer-silent structural/context positions receive a normalized raw-term edge only when no
    semantic edge covers them, so quoting never erases a lexical position.

  An unquoted `red shoe` remains `red AND shoe` and is order-independent. A negated quoted clause
  rejects only when the whole adjacent path is present.

- **Decision — token graphs, not token arrays.** Query phrase edges carry
  `(start-position, end-position, alternative FeatureIds)`. Title edges carry the same position span
  and one `FeatureId`. Ordinary tokens are `i → i+1`; a configured multi-word entity can be one
  longer edge. Graph-language intersection advances one query edge and one title edge with a common
  label. Edge spans need not have equal lengths, so a declared `ny ↔ new york` alias can map the
  one-token and two-token paths without weakening adjacency around either form.

- **Decision — preserve the ADR-061 polarity split.** Required phrases are checked against the
  overlapping positive graph `P(T)`: query-side equivalence expansion widens edge labels, and
  title-side aliases/entities add alternate graph paths. `P(T)` unions the canonical path,
  force-additive analysis, positioned raw-token readings, and overlapping entity edges; retaining
  every live grader start preserves alternate stateful paths. Forbidden phrases are checked against
  the canonical leftmost-longest graph `N(T)` and are never equivalence-expanded. Thus
  `"new york" knicks` can match `ny knicks`, while `foo -"new york"` retains the established
  canonical behavior on `foo new york city`.

- **Decision — candidate-only, default-visible retrieval proxy.** Every label on a required phrase
  graph enters one candidate-only proxy family. It is deliberately absent from the flat exact
  any-of columns: otherwise enabling one unrelated quoted row could make a graph-hole label satisfy
  an ordinary bare-term query. A title satisfying the phrase traverses at least one labeled edge and
  therefore hits at least one proxy signature; exact graph intersection remains the sole phrase
  truth condition. Phrase covers use the always-visible main lane even when each individual label
  is top-64-hot—the phrase's semantic selectivity must not be confused with proxy-label cost.
  A cluster replicates any phrase-proxy cover as `ReplicatedAlwaysVisible`, including a mixed query
  whose sole flat required term is top-64-hot and would otherwise be class C. Thus graph-only labels
  never become selective ring-placement keys that flat coordinator routing cannot see. Forbidden
  graph labels never participate in signatures.

- **Decision — extend the narrow integer program.** ADR-119 predicate-program v1 is unchanged for
  compound any-of rows. Program v2 appends canonical required and forbidden phrase graphs:

  ```text
  required-phrase-count
    final-position arc-count
      start end alternative-count feature-id...
  forbidden-phrase-count
    final-position arc-count
      start end alternative-count feature-id...
  ```

  Mmap open validates bounds, canonical edge/label order, and a complete query path before
  publication. Scalar verification uses only integer words, positioned `FeatureId` edges, and
  reusable match scratch; there are no strings or AST interpretation on the match path. Phrase-free
  snapshots keep the existing flat normalization/verifier path. A phrase-bearing snapshot routes
  the broad/hot batch lane through the scalar positioned verifier rather than the flat bitmap
  kernel; a columnar token-graph transpose is a performance follow-up, not a correctness shortcut.
  The snapshot capability gate counts only live phrase rows in memory and mmap segments, so deleting
  the last quoted query immediately restores the phrase-free/columnar path without awaiting
  compaction.

- **Decision — bounded complexity fails open.** Graph intersection admits at most 65,536 visited
  `(query-position, title-position)` states and 65,536 charged query/title arc inspections per
  candidate. Positioned positive analysis retains up to 64 live starts per canonical grader;
  exceeding that crafted-title guard marks the graph incomplete rather than discarding a path
  silently. Missing/incomplete positioned context, scratch re-entry, or either graph budget's
  exhaustion is interpreted by polarity: a required phrase does not reject and a forbidden phrase
  does not trip. Explain applies the same budgets and polarity rule, so it cannot contradict the
  verifier. That can over-match, but can never create a false negative. Persisted query graphs
  themselves are validated and bounded by the existing parse/compiled-column limits.

- **Decision — persistence and compiler fence.** `.seg` v10 uses the cumulative v9 predicate columns
  and admits program v2; it is written only while a quoted predicate exists. Compiler semantics
  advances from 2 to 3: semantics 0 lost clause boundaries, semantics 1 fixed boundaries,
  semantics 2 preserved complete any-of members but flattened quotes, and semantics 3 preserves
  phrase adjacency. Live semantics-0/1/2 standalone or local-cluster materializations source-rebuild
  before serving. Raw attach, shard-local restart, and mixed-peer recovery continue to fail closed
  through the existing manifest/checkpoint/fingerprint/wire attestations.

- **Decision — explain and every oracle see the same contract.** Structured explain exposes
  `required_phrases` and `forbidden_phrases` as positions and labeled arcs, and reports
  `required_phrase[i] not contiguous` / `forbidden_phrase[i] present`. Every shared-front-end brute
  harness now evaluates positioned graphs. The zero-dependency reference matcher independently
  implements positioned analysis and graph intersection.

- **Correctness argument.** Let a title satisfy required phrase graph `Q`. Its matching title path
  contains at least one edge label from `Q`; that label is in the candidate-only positive proxy
  family, and `match_phrase_views` adds every positive graph label to the separate probe-visible
  feature set. The proxy family therefore retrieves the query without widening flat exact
  semantics. Exact graph intersection accepts the connected path.
  Forbidden graphs are absent from retrieval and can only reject after the query is already a
  candidate. Alias expansion only adds positive labels/paths. Every phrase-proxy cluster placement
  is replicated, so coordinator routing cannot miss its owner. Consequently the implementation may
  retrieve or accept extra work under its explicit fail-open guard, but cannot drop a true match.

- **Alternatives declined.**
  - Rename quotes to “grouped terms”: honest about the old implementation but incompatible with the
    documented DSL and familiar ES/OpenSearch ergonomics.
  - Compare raw token strings: violates strings-die-at-compile-time and bypasses typed normalization.
  - Store a flat feature sequence: cannot represent multi-word aliases or overlapping analyzer paths
    without either false negatives or a combinatorial path expansion.
  - Force every query through the positioned verifier: needlessly taxes phrase-free corpora.
  - Treat aliases as unordered alternatives: restores the original conjunction bug around the alias.

- **Proof.** Hand-authored tests pin required/forbidden order and adjacency, default split vs
  configured fold punctuation, alias compression/expansion, stateful raw-token paths, repeated
  grader starts, default visibility, graph-label isolation from bare rows, replicated cluster
  routing, scalar vs requested-columnar batch parity, and structured explain. PIT tests distinguish
  reordered positional inputs. Predicate-program units cover v2 truth and malformed graphs. `.seg`
  v10 round-trip/malformed refusal and semantics-2 source migration pin persistence. The independent
  oracle covers plain and alias-bearing quotes; the randomized single-node, cluster, durability,
  stress, coverage-gap, and gRPC harnesses all evaluate the positioned semantic predicate.
