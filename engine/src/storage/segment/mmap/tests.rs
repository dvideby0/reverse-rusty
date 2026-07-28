use super::*;

// Well-formed per-query columns for ONE untagged query against empty blobs:
// one entry each, every offset/len landing inside a zero-length blob. Calls
// `validate_columns` for that one query at `version` with the given tag columns.
fn validate_one_query(version: u32, tag_off: &[u32], tag_len: &[u16]) -> io::Result<()> {
    let off1 = [0u32];
    let len1 = [0u16];
    validate_columns(
        version,
        1,
        &off1,
        &len1,
        0,
        &off1,
        &len1,
        0,
        &off1,
        &len1,
        &[],
        &[],
        0,
        tag_off,
        tag_len,
        0,
        &[],
        &[],
        &[],
    )
}

/// A v3+ segment with `num_queries > 0` MUST carry a per-query tag column (the
/// writer pushes one entry per query, length 0 when untagged). A zero-length tag
/// column on such a file is corruption — e.g. a torn write that re-passes CRC —
/// and must fail loud rather than silently read every query back as untagged
/// (which would drop tagged queries from filtered percolation). Codex review.
#[test]
fn v3_with_queries_requires_tag_column() {
    let err = validate_one_query(3, &[], &[])
        .expect_err("v3 + queries + empty tag column must fail loud");
    assert_eq!(err.kind(), io::ErrorKind::InvalidData);
}

/// The same empty tag column on a pre-tag v1/v2 file is legitimate (the section
/// did not exist), so it must still open — back-compat is preserved.
#[test]
fn v2_with_queries_allows_empty_tag_column() {
    validate_one_query(2, &[], &[]).expect("v2 untagged column must still validate");
}

#[test]
fn v6_rejects_priority_column_count_mismatch() {
    let path = std::env::temp_dir().join(format!(
        "reverse_rusty_bad_rank_column_{}.seg",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let norm = crate::normalize::Normalizer::default_vocab().expect("normalizer");
    let mut dict = crate::dict::Dict::new();
    let ast = crate::dsl::parse("acme chrome").expect("query");
    let mut lc = String::new();
    let ex = crate::compile::extract(&ast, &norm, &mut dict, &mut lc);
    dict.finalize_mask();
    let mut segment = crate::segment::Segment::new();
    segment
        .add_compiled_ranked(
            &ex,
            &[],
            &dict,
            1,
            1,
            crate::rank::RankValues { priority: 9 },
            crate::segment::CompileKnobs {
                accept_class_d: false,
                hot_anchor_threshold: 0,
                dedup_bodies: true,
            },
        )
        .expect("accepted query");
    crate::storage::write_segment(&segment, &path).expect("write v6");

    let mut bytes = std::fs::read(&path).expect("segment bytes");
    let mut cursor = read_u64_at(&bytes, 16).expect("exact offset") as usize;
    for kind in [8u8, 8, 4, 2, 4, 4, 2, 4, 4, 2, 4, 2, 4, 4, 8] {
        cursor = match kind {
            8 => read_u64_slice(&bytes, cursor).expect("u64 column").1,
            4 => read_u32_slice(&bytes, cursor).expect("u32 column").1,
            2 => read_u16_slice(&bytes, cursor).expect("u16 column").1,
            _ => unreachable!(),
        };
    }
    // `cursor` is the appended priority array's count word. Keep the file
    // CRC-valid so open reaches the structural count validation.
    bytes[cursor..cursor + 4].copy_from_slice(&0u32.to_le_bytes());
    let n = bytes.len();
    let crc = crate::storage::crc32(&bytes[..n - 4]);
    bytes[n - 4..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, bytes).expect("rewrite malformed segment");

    let error = MmapSegment::open(&path).expect_err("rank count mismatch must fail loud");
    assert_eq!(error.kind(), io::ErrorKind::InvalidData);
    assert!(error.to_string().contains("priority column length"));
    let _ = std::fs::remove_file(path);
}
