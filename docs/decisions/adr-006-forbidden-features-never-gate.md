# ADR-006: Forbidden features never gate (structural enforcement)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** Gating on MUST_NOT features is tempting (they look selective) but lethal for
  correctness — a title that *lacks* a forbidden feature would not be retrieved, causing a
  false negative.
- **Decision:** The signature optimizer literally cannot see forbidden features. They exist
  only in the exact-match plan. This is enforced structurally (code path), not by convention.
- **Consequence:** Zero false negatives for the MUST_NOT case, by construction. The
  differential oracle verifies this over millions of (title, query) pairs.
- **See also:** The correctness contract in [design/README.md](../design/README.md) §2

