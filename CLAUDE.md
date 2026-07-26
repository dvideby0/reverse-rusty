# CLAUDE.md — Reverse Rusty compatibility entry point

The canonical agent instructions are [`AGENTS.md`](AGENTS.md). Read that file completely before
changing this repository; it owns the correctness invariants, commands, repository map, and
task-to-document router.

The load-bearing contract is repeated here only so it cannot be missed:

> **Lossless signature cover:** if a title `T` *could* satisfy query `Q`'s positive semantics, then
> `T` must generate at least one signature that retrieves `Q` from the candidate index.

Do not duplicate current behavior, versions, performance captures, shipped status, or proposals in
this compatibility file. Their canonical homes are indexed from
[`docs/README.md`](docs/README.md).
