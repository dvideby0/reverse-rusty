# ADR-069: Parity-mode number typing — the `pop` demotion becomes a configurable number-context word list

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** [ADR-064](adr-064-percolator-drop-in-parity-audit.md) item 3. The normalizer's number
  typing is **position-sensitive** in exactly one place that no configuration can reach: a hard-coded
  rule demotes a number immediately after the token `pop` to a generic term (`pop 1995` → `term:1995`,
  never `year:1995` — the real-eBay population hardening, [normalization.md](../design/normalization.md)
  §4). Against a position-*insensitive* reference matcher (a regex over the raw title), that is the
  audit's **one residual false-negative class**, demonstrated in both directions: a query-side year
  (`year:1995`) cannot match a title-side `pop`-adjacent `term:1995`, and a query-side `pop`-adjacent
  `term:1995` cannot match a title-side `year:1995`. The other number-context rules — `#` card-numbers
  and `/` serials — already ride the configurable punctuation table (declaring `#`/`/` as `split`
  removes the marker tokens, ADR-058), which is exactly what the documented parity configuration does
  ([percolator-workload.md](../research/percolator-workload.md) §Drop-in parity). `pop` was the last
  context rule not behind configuration.

- **Decision.** Generalize the hard-coded rule into a **number-context word list** on the shared
  normalizer — the ADR-058 move (hard-coded behavior → configuration, default byte-identical), not a
  one-off boolean:

  1. **Normalizer / builder.** `Normalizer` carries `number_context: Vec<String>` (lowercased at
     build); the emit pipeline's number branch checks the previous token against the list instead of
     the literal `"pop"`. `NormalizerBuilder::set_number_context_words` (+ fluent
     `number_context_words`) replaces the list; a builder that never calls it resolves to **`["pop"]`**
     — the historical rule, byte-identical (the default list is one entry, so the hot-path comparison
     count is unchanged). An **empty list disables the rule** — the parity mode: number typing becomes
     position-insensitive, a 4-digit 1900–2099 token is `year:N` everywhere. A custom list works too
     (`["qty"]` demotes after `qty` and not after `pop`) — the rule is domain vocabulary, not a flag.
  2. **Vocab persistence.** `Vocab.number_context: Option<Vec<String>>` (serde-defaulted): `None` —
     the default, and the shape of every pre-ADR-069 vocab JSON — leaves the builder untouched
     (byte-identical); `Some([])` is the persisted parity knob. It rides everything the punctuation
     table rides: `PUT /_vocab`, the manifest `vocab_data` blob (survives reopen), and the live apply
     path — single-node `set_vocab` recompiles stored queries under the new typing
     (`recompile_stale_segments`, the ADR-046 mech-2 machinery), the in-process cluster `set_vocab`
     does its blue/green re-place. `Vocab::merge` is first-wins like every other field (an
     explicitly-set list survives; an unset vocab adopts the other's). The same list runs over queries
     and titles — the §2 shared-normalizer invariant is what makes the knob *close* the FN class
     rather than move it.

- **Evaluated and declined: emitting both typings title-side in parity mode.** The ADR-064 decision
  asked for an evaluation of a recall-superset variant — the title emits `year:N` **and** `term:N` for
  a 4-digit year, so it matches a query compiled under *either* typing. Declined, two reasons:
  1. **It protects a state that cannot arise.** Both-typings only helps when the query side and title
     side type the same number differently — but the same normalizer runs both sides, every vocab
     change recompiles stored queries (single-node recompile / cluster blue/green), and the vocab is
     manifest-persisted so a reopen cannot desync the two. With the knob applied, both audit
     directions close with *consistent* typing (test-proven); there is no residual mixed state for
     both-typings to rescue.
  2. **It is not actually FP-only.** In the single-view path one title feature set serves retrieval
     *and* forbidden checks. Adding `term:N` alongside `year:N` flips forbidden-clause outcomes
     wherever a query can still express a demoted year (`-#1995` under a `Marker` `#` forbids
     `term:1995`; a both-typings title with a year-position `1995` would now be rejected) — a
     semantics change beyond parity, the opposite of recall-superset. Containing it would mean
     routing the extra typing into the positive view `P(T)` only — i.e. forcing every title through
     the ADR-061 dual-view path even with no aliases active — a hot-path cost bought for case (1),
     which is already empty.

- **Scope + the one visible trade.** Single-node and the in-process cluster get the knob through the
  normal vocab machinery; a *runtime* flip on a non-local (gRPC) or tagged cluster is refused exactly
  like every other vocab change (the ADR-046/055 deferrals — no new restriction; a cluster *built*
  with a parity vocab works throughout). The trade: the knob removes `pop`'s number-context role
  **entirely**, so in a graders-configured vocabulary `psa pop 7` reads as a PSA grade once the rule
  is off (the population count is no longer shielded from the pending grader). The parity mode targets
  the documented parity configuration — an **empty** vocabulary, no graders — where the case cannot
  arise; the interaction is pinned by a characterization test so it stays a documented consequence,
  not a surprise.

- **Why this is safe (the correctness contract).** A normalizer with a different number-context list
  is just a *different shared normalizer* — the lossless-cover contract is normalizer-relative, and
  the same list runs over queries and titles, so the feature spaces stay aligned and the cover holds
  under any setting (the ADR-058 argument verbatim). Signature gating, the candidate index, and the
  verifier are untouched; only feature *names* change (`term:1995` ⇄ `year:1995`). Default
  byte-identical three ways: the builder default is the literal historical rule, `Vocab.number_context
  = None` leaves the builder untouched, and legacy vocab JSON deserializes to `None`.

- **Testing.** Golden pins (`normalize/tests.rs`): the historical demotion pinned, the empty-list
  position-insensitive typing, the custom-list generalization, and the graders-on characterization.
  Vocab (`vocab/tests.rs`): JSON round-trip preserves `Some([])` as distinct from `None`, legacy JSON
  ⇒ default behavior, merge first-wins. Oracle (`tests/oracle/vocab.rs`): engine ≡ brute (zero FN/FP)
  under the parity normalizer including forbidden-year and any-of paths; **both audit FN directions
  asserted closed** with a default-normalizer contrast proving the knob does the work; and the **live
  flip** — `set_vocab` with the knob recompiles already-stored queries (match appears), restoring a
  default vocab re-demotes (match disappears) — the "vocab-persisted" claim, end to end.

- **See also:** ADR-064 (the program; item 3), ADR-058 (the precedent: byte-cleaning behavior →
  configuration, and the `#`/`/` half of position-insensitive parity), ADR-046 (the vocab apply/
  recompile machinery the knob rides), [`normalization.md`](../design/normalization.md) §2/§4,
  [`percolator-workload.md`](../research/percolator-workload.md) §Drop-in parity (the parity
  configuration this completes).
