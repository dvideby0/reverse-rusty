# ADR-119: Preserve multi-token any-of member semantics

> [Distributed v1 — the ADR-065 graduation program decisions](areas/distributed-v1-graduation.md) · [Decision hub](../DECISIONS.md) · **Status:** Accepted

- **Context.** The DSL parser retains each comma-delimited any-of member as one string, but the
  compiler previously normalized a member to several features and kept only its rarest feature.
  That feature was used both as a lossless retrieval proxy and as the exact truth condition. The two
  roles are not equivalent. `(red shoe,boot)` could accept `shoe` without `red`, and
  `-(red shoe,boot)` flattened to `NOT red AND NOT shoe AND NOT boot`, rejecting `red hat` even
  though neither complete member matched. Two independently implemented matchers copied the same
  underspecified assumption, so a differential alone did not expose it.

- **Decision — explicit Boolean contract.** An unquoted multi-token member is a conjunction of its
  independently normalized requirements; a group is a disjunction of complete members. Therefore:

  ```text
  (red shoe,boot)          = (red AND shoe) OR boot
  -(red shoe,boot)         = NOT ((red AND shoe) OR boot)
  ```

  Top-level clauses remain ANDed. A configured multi-word entity may naturally normalize one member
  to one feature. This decision does not define quoted-phrase adjacency; that is a separate language
  rule.

- **Decision — separate retrieval necessity from exact sufficiency.** For each positive semantic
  member, the compiler may select its rarest required feature as a retrieval proxy. The resulting
  proxy group remains in the ordinary any-of SoA and drives signature selection, placement, and the
  batch prefilter. It is only a necessary condition. When any member has multiple requirements, the
  compiler also retains the complete OR-of-AND predicate for exact verification. A shared proxy does
  not merge distinct semantic members. A one-member group becomes an ordinary conjunction.

- **Decision — equivalences widen one requirement at a time.** ADR-054 expansion changes a
  requirement from one feature to an OR of equivalent features; it never flattens the member's other
  conjuncts. For example, `[[open], [box]]` may become `[[open,opened], [box]]`. Forbidden predicates
  are not equivalence-expanded, preserving ADR-054/061's negative policy.

- **Decision — integer-only exact program.** The common single-feature case stays entirely in the
  existing struct-of-arrays layout. A compound query receives one canonical `u32` program:

  ```text
  version
  positive-group-count
    member-count
      requirement-count
        alternative-count feature-id...
  negative-conjunction-count
    feature-count feature-id...
  ```

  Scalar verification interprets only integer words against sorted `P(T)`/`N(T)` feature slices.
  The columnar broad/hot path evaluates the same shape with reusable bitmaps. There are no strings,
  AST nodes, allocations, or virtual calls on the match path. Persisted programs are structurally
  validated before mmap publication. The broad prefilter deliberately checks only required features
  and proxy groups; ignoring the nested program there can only perform extra work, never skip a
  possible match.

- **Decision — persistence and upgrade fence.** `.seg` v9 appends per-row predicate offsets/lengths
  and the shared program blob, and is written only when a compound predicate exists. It cumulatively
  includes the v6–v8 columns, so an older reader refuses rather than silently ignoring the exact
  semantics. Compiler semantics advances from 1 to 2 independently of the layout version:
  semantics 0 lost clause boundaries, semantics 1 preserved those boundaries but lowered
  multi-token members to proxies, and semantics 2 preserves complete members. Every live semantics
  0/1 standalone or local-cluster materialization is source-rebuilt before serving. Raw shard attach,
  shard-local restart, and recovery from an older peer fail closed because only the coordinator can
  re-place a rebuilt corpus atomically. Existing cluster-manifest, shard-checkpoint, fingerprint, and
  recovery-handshake stamps carry the new current value, so no additional manifest or wire layout is
  needed.

- **Decision — carry the predicate through every representation.** In-memory and mmap exact stores,
  mechanical/re-anchoring compaction, canonical-body hash/equality, scalar and batch verification,
  raw reconstruction, persistence, and explain all include the canonical program. Explain preserves
  the existing compact `anyof_groups` field as the proxy view and adds complete positive-member and
  negative-conjunction fields.

- **Correctness argument.** For each positive member `m`, its proxy is selected from a requirement
  that every title satisfying `m` must contain (or from that requirement's equivalence alternatives).
  The cover emits a signature for every member proxy, so a title satisfying any member retrieves the
  query. No forbidden feature participates in retrieval. Exact verification then accepts only when
  one whole positive member per group is satisfied and rejects only when one whole forbidden member
  is satisfied. Thus proxies may add candidates but cannot cause a false negative or alter Boolean
  truth.

- **Alternatives declined.**
  - Keep rarest-member proxies as exact predicates: fast but semantically wrong.
  - Flatten every normalized feature into the group: changes `(a b,c)` into `a OR b OR c`.
  - Expand nested logic into a Cartesian family of ordinary SoA groups: grows combinatorially and
    still cannot express whole-member negation cleanly.
  - Put every query behind a general bytecode VM: unnecessary overhead for the overwhelmingly common
    flat predicate; the optional narrow program keeps that path unchanged.

- **Proof.** Compiler goldens pin mutable/read-only extraction, shared proxies, singleton promotion,
  and requirement-local equivalence expansion. Hand-authored independent-oracle cases pin the four
  human truth-table outcomes for positive and negated groups. Scalar exact-program units, broad/hot
  batch-versus-scalar tests with prefilter/materialization toggles, class-D negative-only coverage,
  mmap v9 round-trip and malformed-program refusal, compiler-semantics-1 source migration, and the
  randomized single-node/cluster/durability/gRPC oracles cover every execution and persistence path.
