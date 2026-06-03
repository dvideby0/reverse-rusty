# ADR-005: Typed errors over stringly-typed Results

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted


- **Context:** Early on, Reverse Rusty used `Result<_, String>` for parse failures and silently dropped
  rejections during ingest. This made debugging and accounting difficult.
- **Decision:** Introduce `ParseError { kind, pos }` with a `#[non_exhaustive]`
  `ParseErrorKind` enum implementing `Display` + `std::error::Error`. Ingest paths return
  `IngestReport` with separate counts for parse rejections vs class-D rejections. Added
  `try_insert_live` that surfaces typed errors.
- **Consequence:** Callers get inspectable, composable errors. No `unwrap()` in library code.
  Rejection accounting is accurate. Back-compat preserved via `insert_live` wrapper.

