# ADR-011: Cache-line blocked bloom over binary fuse / u64-blocked bloom

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** The multi-segment LSM layout requires each incoming title to probe every segment.
  Read amplification is proportional to segment count. Per-segment anchor filters can skip
  probes that would definitely miss. The comparison point for each probe is a hash-map miss
  with an identity hasher — approximately one memory access (~1 cache line). Three approaches
  were evaluated:
  (A) **Binary fuse filters** (xorf crate): 3 parallel memory accesses, ~9 bits/key, ~0.4%
      FPR. Best space efficiency. Adds a 4th dependency, requires key deduplication, and
      construction can fail (needs retry with a different hash seed).
  (B) **Cache-line blocked bloom** (RocksDB Full Filter, Putze et al. 2007): 1 cache-line
      access (512-bit block = 64 bytes), ~10 bits/key, ~1% FPR. No new dependency. Proven
      over 10+ years in RocksDB production.
  (C) **u64-blocked bloom** (our initial attempt): 1 memory access, ~16 bits/key, ~0.5–2%
      FPR. Non-standard 64-bit block size is too narrow — only 5 fingerprint bits fit,
      leading to variable FPR under load.
  An earlier attempt with a classic scattered-probe bloom (7 random probes) made throughput
  **worse** than no filter (620k→167k at 8 segments vs baseline 779k→461k), confirming that
  multiple cache-line accesses per check exceed the hash-map miss budget. RocksDB abandoned
  scattered-probe blooms for the same reason circa 2014.
- **Decision:** Cache-line blocked bloom (Option B). 512-bit blocks with 6 probes via
  Kirsch-Mitzenmacher double hashing. No external dependency. Construction never fails.
  One cache-line access per check.
- **Consequence:** Filter skip rates scale properly with segment count (47%→87% at K=1→8).
  Memory is compact (~0.07 MB for 300k queries across 8 segments). Zero false negatives
  (verified by oracle). In-memory, net throughput improvement is modest because the filter
  check cost (~1 cache-line) approximately equals the hash-map miss it replaces. The real ROI
  comes when segments are mmap'd from disk — a hash-map miss against an mmap'd segment is a
  potential page fault (microseconds), while the in-memory filter check stays at nanoseconds.
  No percolation system in the literature (Lucene Monitor, ES Percolator, Luwak) implements
  segment-skip filters — this is novel to Reverse Rusty.
- **See also:** [ingestion-and-updates.md](../design/ingestion-and-updates.md) §6

