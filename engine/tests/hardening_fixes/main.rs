//! Integration tests for the three hardening fixes:
//!   1. Vocab-epoch staleness enforcement after set_vocab()
//!   2. Zero unwrap() — corrupt data handled gracefully in storage/WAL
//!   3. Per-segment reverse index makes delete_by_logical_id O(segments)
//!
//! This file exercises all three interacting simultaneously: build → vocab change
//! → delete → flush → compact → persist → reopen → verify correctness throughout.

mod harness;

mod combined_lifecycle;
mod corrupt_data;
mod delete_reverse_index;
mod explain_hit;
mod vocab_epoch;
