# ADR-073: REST surface honesty — tag-value coercion, flush wiring, per-request broad

> [Back to the decisions index](../DECISIONS.md)


- **Status:** Accepted (2026-06-10). Closes [ADR-064](adr-064-percolator-drop-in-parity-audit.md)
  items **4, 5, and 6** — the three remaining decided items of the drop-in-parity work package.
  Batched under one ADR/PR (the ADR-052 precedent for small, same-theme hardening items; the program
  ADR's "own ADR/PR each" intent was sized for the semantic changes, items 1–3): all three turn a
  *silent* REST behavior loud or honest, none touches signature gating, and together they are a few
  hundred lines including tests.
- **Context:** The ADR-064 audit found three places the REST surface lied quietly. **(4)** Ingest
  silently dropped a non-string tag value (`{"tags": {"priority": 7}}` ingested with *no* priority
  tag and no error) and filter arrays silently dropped non-string elements — while a *scalar*
  non-string filter value 400'd. The silent half corrupts filtered percolation invisibly: the query
  becomes unreachable by any filter on that key. **(5)** `put_doc` →
  `try_upsert_live_with_tags` bypassed the only `maybe_flush` call site (the infallible
  `insert_live_with_tags` wrapper), so `memtable_flush_threshold` was **inert for REST single-doc
  writes** — WAL-durable, but memtable + WAL grew until a manual `/_flush`; the knob lied. **(6)**
  `/_search` honored only the server-wide `--include-broad` and **silently ignored** an
  `include_broad` body field (serde unknown-field tolerance) while `/_mpercolate` and both cluster
  handlers (ADR-070) had the per-request override — with broad off, class-C hits read as missing
  data.
- **Decision (4) — canonical scalar coercion, loud rejects for the rest, ONE rule on both sides.**
  The audit allowed "reject or canonically coerce — pick one." Picked **coercion** for scalars:
  the reference workload's dominant filter key is a category (numeric IDs in the wild), and ES
  itself coerces numbers/bools onto keyword fields — a drop-in replacement that 400s payloads the
  reference accepted is not a drop-in. The contract:
  - **string** → itself; **number / bool** → canonical JSON text (`7` → `"7"`, `true` → `"true"`).
    One shared function (`coerce_tag_scalar`, `bin/server/handlers/doc.rs`) serves ingest **and**
    both filter parsers (`search/resolve.rs`), so the two sides can never disagree about a coerced
    form. The known dark corner is pinned, not hidden: `7.0` coerces to `"7.0"` — a *different* tag
    than `7` — exactly as in ES.
  - **null** → the ES "no value": skipped on ingest (top-level value or array element; `"tags":
    null` = no tags) but a **400 in a filter** — an unanswerable predicate must never be silently
    dropped, because dropping a filter clause *widens* the result set.
  - **object / nested array / non-object `tags` field** → **400** everywhere (`/_bulk`: per-item),
    where all of these were previously silent. Applies identically in cluster mode (the handlers
    share `extract_ingest_tags` / `resolve_percolate`).
- **Decision (5) — the knob moves into the fallible write paths.** `maybe_flush` now runs at the
  *success* tail of `try_insert_live_with_tags` and `try_upsert_live_with_tags` (accepted outcomes
  only — a rejected/failed write never flushes), and the infallible wrapper's duplicate call is
  dropped. Every live write path honors `memtable_flush_threshold`; an embedded caller that wants
  manual control sets the threshold (that is what the config is for — the test suites that pin
  WAL-tail behavior already set `usize::MAX`). Deliberately untouched: WAL **replay**
  (`replay_insert`/`replay_upsert` — recovery must not commit segments mid-replay), **bulk** (ADR-017
  builds sealed segments directly, no memtable), and the **cluster shard funnel** (checkpoint-driven
  durability, ADR-031/032).
- **Decision (6) — `include_broad: Option<bool>` on `/_search`**, falling back to the server
  default, both single- and multi-doc arms — the exact `/_mpercolate` semantics. The audit's
  "consider rejecting unknown body fields" is **declined**: `PUT /_doc` *deliberately* treats
  unknown fields as ES-style sibling tags, and `deny_unknown_fields` on the percolate bodies would
  break ES clients sending benign envelope extras; the cost of tolerance was the one field that
  changed results, which now exists.
- **Why this is safe:** none of the three touches signature gating, the candidate index, or the
  verifier — the lossless-cover contract is untouched. (4) is REST-boundary-only (the engine API
  was always `(String, String)`); coercion makes previously-dropped tags *exist* (filtering only
  ever removes matches, so a new tag can only make filtered results more correct) and previously-400
  filter scalars work; nothing that previously *worked* changes shape. (5) changes *when* a flush
  happens, not what is durable (the WAL already covered every acknowledged write); flush is the
  same crash-safe path the wrapper always ran. (6) is opt-in per request; absent field ⇒
  byte-identical to the server default.
- **Proven:** handler tests — ingest coercion units (scalars/null-skip/structured-reject), PUT/bulk
  400s, the load-bearing **ingest-meets-filter agreement** (a category ingested as `7` is reachable
  by filter `7`, `"7"`, `[7]`, and the ES `terms` envelope; `8` does not match), unanswerable-filter
  400s across all three filter shapes, threshold-2 REST PUTs auto-flush with matching intact across
  the flush boundary (insert + upsert paths), and both `include_broad` override directions asserted
  against engine truth. Full default suite green (27 suites).
- **See also:** ADR-064 (the program ADR — items 4–6 here close its decided list), ADR-049 (tags +
  filtered percolation), ADR-055 (cluster tags), ADR-070 (the cluster handlers these stay aligned
  with), ADR-052 (the batch-hardening precedent), ADR-013 (WAL fail-closed — why (5) was never a
  durability bug, only an honesty bug).
