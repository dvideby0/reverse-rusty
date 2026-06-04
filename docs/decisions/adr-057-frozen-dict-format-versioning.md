# ADR-057: Version + harden the frozen-space serializations (feature dict + tag dict)

> [Back to the decisions index](../DECISIONS.md) · **Status:** Accepted

- **Context.** Every on-disk binary format in the engine is self-describing with a `magic + version`
  header — the segment file (`.seg` v3, ADR-012), the engine manifest (`PMAN` v2) and cluster manifest
  (`RCMN` v4) (ADR-031/032/049), the source store (`SRCS` v2, ADR-020) — *except two*: the feature-dict
  serialization (`storage/dict.rs`) and its literal twin the tag-dict serialization (`storage/tagdict.rs`,
  ADR-049). Both opened straight with a bare `[count: u32]`. These blobs are the **frozen feature/tag
  space** — embedded length-framed inside the manifests *and* shipped cross-process over gRPC (`AdoptDict`,
  ADR-034/055). Two concrete hazards followed, both **silent**, which is exactly what this project's
  correctness contract forbids:
  - **Layout drift.** Any future change to the per-record layout (a new per-feature field, a reordering)
    would make an old reader **misparse** an existing blob — there was no version to reject it on.
  - **Kind drift.** `deserialize_dict` decoded the `FeatureKind` byte with `match b { 0..=7 => …, _ =>
    Generic }`. Adding a `FeatureKind` variant (tag 9) and reading an older build's dict — or vice versa —
    **silently downgraded** the feature to `Generic` (a semantic corruption) instead of failing loud.
  - **Latent panic.** Worse, the dict body parser indexed raw (`data[cursor]`) for the kind/mask/finalized
    bytes, so a **truncated or corrupt** blob *panicked* (index out of bounds) — a direct violation of the
    "no panicking in library code" invariant ([`CLAUDE.md`](../../CLAUDE.md)).

  The roadmap's robustness backlog flagged the first as *"Dict format not versioned — adding a new
  `FeatureKind` variant would silently corrupt deserialization."* This ADR closes all three, for **both**
  twins.

- **Decision.** Give each frozen-space serialization a `magic + version` header and a fully-fallible,
  fail-loud parser, while staying byte-for-byte readable for existing on-disk blobs:
  - **Header.** `serialize_dict` now emits `["RDCT"][version: u32 = 1][num_features: u32]…`; `serialize_tagdict`
    emits `["RTGD"][version: u32 = 1][num_tags: u32]…`. The magic's little-endian-u32 value (≈1.4×10⁹ /
    1.1×10⁹) is far larger than any real feature/tag count, so a legacy (header-less) blob — which opens
    with the count — can never be mistaken for a versioned one.
  - **Dispatch + forward-incompat rejection.** `deserialize_dict`/`deserialize_tagdict` sniff the first 4
    bytes: `== MAGIC` ⇒ read the version (`version == 0` or `> CURRENT` ⇒ a loud `InvalidData` error naming
    the version — *"written by a newer Reverse Rusty"*); otherwise parse the **legacy v0** (header-less)
    body. v1's body is byte-identical to v0, so **one** body parser serves both — the version exists to make
    a *future* divergent layout `v2` branch and fail-loud on old builds, exactly the `sources.rs` pattern.
  - **One canonical kind table.** The `FeatureKind ↔ u8` mapping is now the single `dict::kind_tag` /
    `dict::kind_from_tag` pair (`pub(crate)`), **shared with `Dict::fingerprint`** — so the fingerprinted
    kind and the persisted kind can never drift. `kind_from_tag` is the *strict* inverse: an unrecognized
    tag returns `None`, which the parser turns into a loud error rather than a silent `Generic`. The
    duplicate `kind_to_u8`/`u8_to_kind` in `storage/dict.rs` are deleted.
  - **No panics.** Both body parsers are now fully fallible — every read goes through the bounds-checked
    `read_u16_at`/`read_u32_at` or `data.get(…).ok_or_else(…)`; the trailing finalized flag is
    `data.get(cursor) == Some(&1)`. A truncated/corrupt blob yields `InvalidData`, never a panic.

- **Why it is back-compatible (the load-bearing property).** Existing data opens unchanged:
  - **Persisted manifests.** The dict/tag-dict blobs are stored **length-framed** inside the manifests
    (`write_u32(len)` + bytes), so changing the blob's *internal* bytes is invisible to the manifest format
    — a v0 dict blob written by an older build still deserializes via the header-sniff's legacy branch
    (proven by stripping the 8-byte header off a fresh blob *and* by a hand-built v0 blob in the unit tests).
  - **Empty tag space.** The tag-dict's pre-ADR-049 contract — an **empty** blob (a manifest predating the
    tag space) reads back as an empty dict — is checked first, before the magic sniff, so it is preserved
    exactly.
  - **The cross-process fingerprint is untouched.** `Dict::fingerprint`/`TagDict::fingerprint` are
    **content-based** (they hash names/kinds/mask/finalized directly, never the serialized bytes), so adding
    a serialization header does **not** change a fingerprint. The gRPC dict/tag-dict adoption handshake
    (`AdoptDict` + the divergence guard, ADR-030/034/055) and the persisted `shard.ckpt` fingerprint
    therefore behave identically — a durable shard written by an older build reopens with the same
    fingerprint under the new code.

- **Scope.** The two frozen-space binary serializations are the only unversioned formats; the `Vocab` blob
  is self-describing JSON (serde) and every other binary format was already versioned. No format bytes
  *that already exist on disk* change meaning; only freshly-written blobs gain the 8-byte header. The
  default and cluster paths are otherwise byte-identical (same body, same fingerprints, same APIs).

- **Alternatives.** (1) *Gate the dict format on the enclosing manifest version* — rejected: it couples the
  dict to the manifest and does nothing for the gRPC `AdoptDict` path, which ships the dict bytes raw, not
  through a manifest. The blob must be self-describing. (2) *Bump the manifest version instead of adding a
  dict header* — same problem, and it would force a manifest rev for every dict-layout change. (3) *Keep the
  lenient `_ => Generic` kind decode* — rejected: silently downgrading a feature's kind is precisely the
  corruption this project refuses; an unknown tag means the blob came from a build this one cannot faithfully
  read, which must fail loud. (4) *Version only the feature dict (the named roadmap item)* — rejected as a
  half-fix: the tag-dict is the identical-shaped twin (its own header even says *"Mirrors `super::dict`"*)
  with the identical hazards, so the next `TagDict` field would reintroduce the same silent corruption.

- **Testing.** `dict.rs`: `kind_tag`/`kind_from_tag` are exact inverses over every `FeatureKind` and the
  tags are distinct, guarded by an **exhaustive `match`** so adding a variant fails to *compile* until both
  sides + the test are updated. `storage/dict.rs`: round-trip through the header across all nine kinds
  (incl. fingerprint equality), a header-stripped **legacy v0** blob and a hand-built v0 blob both still
  read, a bumped version is rejected loud, an unknown kind tag is rejected loud (not `Generic`), and
  truncation at *every* prefix length errors without panicking. `storage/tagdict.rs`: the existing
  round-trip + empty-blob test, plus legacy-v0-reads, newer-version-rejected, and truncation-never-panics.
  The persistence, cluster-durability, and distributed gRPC oracles (which round-trip real dicts + tag dicts
  through manifest reopen and `AdoptDict` over the wire) stay green unchanged. Full `check.sh` green.

- **Consequences.** The last unversioned binary formats are now self-describing and fail-loud: a layout
  change or a newer-build blob is **rejected with a clear error** instead of silently misparsed, an unknown
  `FeatureKind` tag is **rejected** instead of silently downgraded to `Generic`, and a truncated/corrupt
  blob **errors** instead of panicking — all while existing on-disk dicts/tag dicts (and the cross-process
  fingerprint handshake) keep working byte-for-byte. Adding a future `FeatureKind` variant or a new
  per-record field is now a guided, compile-time-checked, version-bumped change. Closes the roadmap's "Dict
  format not versioned" robustness item (and its tag-dict twin).
