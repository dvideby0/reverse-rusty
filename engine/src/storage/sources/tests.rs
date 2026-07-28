use super::{MetadataLayout, SourceStore, META_IDX_REC, META_VERSION, SRC_HEADER, SRC_IDX_REC};
use std::sync::atomic::{AtomicU64, Ordering};

static NEXT_PATH: AtomicU64 = AtomicU64::new(0);

#[test]
fn bounded_resident_lookup_checks_length_before_clone() {
    let store = SourceStore::new_resident();
    store.insert(7, "0123456789".to_string());
    assert_eq!(store.get_bounded(7, 9), Err(10));
    assert_eq!(
        store.get_bounded(7, 10).expect("fits"),
        Some("0123456789".to_string())
    );
    assert_eq!(store.get_bounded(8, 0).expect("absent"), None);
}

#[test]
fn bounded_lazy_lookup_checks_mmap_length_before_clone() {
    let path = std::env::temp_dir().join(format!(
        "reverse-rusty-bounded-sources-{}-{}.dat",
        std::process::id(),
        NEXT_PATH.fetch_add(1, Ordering::Relaxed)
    ));
    let resident = SourceStore::new_resident();
    resident.insert(7, "0123456789".to_string());
    resident.write_to(&path).expect("write v2 sources");

    let lazy = SourceStore::open(&path, false).expect("mmap sources");
    assert_eq!(lazy.get_bounded(7, 9), Err(10));
    assert_eq!(
        lazy.get_bounded(7, 10).expect("fits"),
        Some("0123456789".to_string())
    );

    std::fs::remove_file(path).expect("remove test sources");
}

#[test]
fn generation_attested_lookup_rejects_resident_base_and_overlay_mismatches() {
    let resident = SourceStore::new_resident();
    resident.insert_document_with_generation(7, "resident".to_string(), 1, 20, &[]);
    assert_eq!(
        resident
            .get_bounded_at_generation(7, 20, usize::MAX)
            .expect("matching generation"),
        Some("resident".to_string())
    );
    assert_eq!(
        resident
            .get_bounded_at_generation(7, 19, usize::MAX)
            .expect("mismatch"),
        None
    );

    let path = std::env::temp_dir().join(format!(
        "reverse-rusty-generation-sources-{}-{}.dat",
        std::process::id(),
        NEXT_PATH.fetch_add(1, Ordering::Relaxed)
    ));
    resident.write_to(&path).expect("write lazy base");
    let lazy = SourceStore::open(&path, false).expect("open lazy base");
    assert_eq!(
        lazy.get_bounded_at_generation(7, 20, usize::MAX)
            .expect("base generation"),
        Some("resident".to_string())
    );
    lazy.insert_document_with_generation(7, "overlay".to_string(), 2, 21, &[]);
    assert_eq!(
        lazy.get_bounded_at_generation(7, 20, usize::MAX)
            .expect("stale snapshot"),
        None,
        "a newer overlay must shadow rather than leak into the old generation"
    );
    assert_eq!(
        lazy.get_bounded_at_generation(7, 21, usize::MAX)
            .expect("overlay generation"),
        Some("overlay".to_string())
    );
    std::fs::remove_file(path).expect("remove test sources");
}

#[test]
fn bounded_lazy_lookup_does_not_touch_document_metadata() {
    let path = std::env::temp_dir().join(format!(
        "reverse-rusty-query-only-sources-{}-{}.dat",
        std::process::id(),
        NEXT_PATH.fetch_add(1, Ordering::Relaxed)
    ));
    let resident = SourceStore::new_resident();
    resident.insert_document(
        7,
        "0123456789".to_string(),
        42,
        &[("tenant".to_string(), "acme".to_string())],
    );
    resident.write_to(&path).expect("write v2 sources");

    let mut lazy = SourceStore::open(&path, false).expect("mmap sources");
    let SourceStore::Lazy {
        base: Some(base), ..
    } = &mut lazy
    else {
        panic!("expected lazy mmap base");
    };
    // Poison only the metadata layout after open. Query-only lookup must
    // still succeed because it reads the original query index/blob alone.
    base.metadata = Some(MetadataLayout {
        version: META_VERSION,
        record_size: META_IDX_REC,
        directory_off: usize::MAX,
        blob_off: usize::MAX,
    });
    assert_eq!(
        lazy.get_bounded(7, 10).expect("query-only lookup"),
        Some("0123456789".to_string())
    );

    std::fs::remove_file(path).expect("remove test sources");
}

#[test]
fn resident_bytes_counts_each_tag_vector_backing_allocation() {
    let tuple_bytes = std::mem::size_of::<(String, String)>();

    let resident_plain = SourceStore::new_resident();
    resident_plain.insert_document(7, String::new(), 1, &[]);
    let resident_tagged = SourceStore::new_resident();
    resident_tagged.insert_document(7, String::new(), 1, &[(String::new(), String::new())]);
    assert!(
        resident_tagged.resident_bytes() >= resident_plain.resident_bytes() + tuple_bytes,
        "resident accounting must include the tags Vec backing allocation"
    );

    let lazy_plain = SourceStore::empty(false);
    lazy_plain.insert_document(7, String::new(), 1, &[]);
    let lazy_tagged = SourceStore::empty(false);
    lazy_tagged.insert_document(7, String::new(), 1, &[(String::new(), String::new())]);
    assert!(
        lazy_tagged.resident_bytes() >= lazy_plain.resident_bytes() + tuple_bytes,
        "lazy-overlay accounting must include the tags Vec backing allocation"
    );
}

#[test]
fn metadata_v1_footer_remains_readable_as_generation_zero() {
    let path = std::env::temp_dir().join(format!(
        "reverse-rusty-metadata-v1-{}-{}.dat",
        std::process::id(),
        NEXT_PATH.fetch_add(1, Ordering::Relaxed)
    ));
    let query = "acme chrome";
    let tags = vec![("tenant".to_string(), "acme".to_string())];
    let mut encoded_tags = Vec::new();
    super::encode_tags(&tags, &mut encoded_tags).expect("encode tags");

    let mut bytes = Vec::new();
    bytes.extend_from_slice(b"SRCS");
    bytes.extend_from_slice(&2u32.to_le_bytes());
    bytes.extend_from_slice(&1u32.to_le_bytes());
    bytes.extend_from_slice(&super::META_HEADER_MARKER.to_le_bytes());
    bytes.extend_from_slice(&7u64.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&(query.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(query.as_bytes());
    let directory_off = bytes.len() as u64;
    bytes.extend_from_slice(&super::TAGS_KNOWN.to_le_bytes());
    bytes.extend_from_slice(&42u32.to_le_bytes());
    bytes.extend_from_slice(&0u64.to_le_bytes());
    bytes.extend_from_slice(&(encoded_tags.len() as u32).to_le_bytes());
    bytes.extend_from_slice(&0u32.to_le_bytes());
    bytes.extend_from_slice(&encoded_tags);
    bytes.extend_from_slice(b"SMET");
    bytes.extend_from_slice(&super::META_VERSION_V1.to_le_bytes());
    bytes.extend_from_slice(&directory_off.to_le_bytes());
    let crc = super::crc32(&bytes);
    bytes.extend_from_slice(&crc.to_le_bytes());
    std::fs::write(&path, bytes).expect("write metadata v1 fixture");

    for retain in [true, false] {
        let store = SourceStore::open(&path, retain).expect("open metadata v1");
        let document = store.get_document(7).expect("document");
        assert_eq!(document.query(), query);
        assert_eq!(document.version(), 42);
        assert_eq!(document.source_generation(), 0);
        assert!(document.metadata_known());
        assert!(document.tags_known());
        assert_eq!(document.tags(), tags);
    }
    std::fs::remove_file(path).expect("remove metadata v1 fixture");
}

#[test]
fn metadata_footer_round_trip_preserves_version_and_canonical_tags() {
    let path = std::env::temp_dir().join(format!(
        "reverse-rusty-metadata-sources-{}-{}.dat",
        std::process::id(),
        NEXT_PATH.fetch_add(1, Ordering::Relaxed)
    ));
    let resident = SourceStore::new_resident();
    resident.insert_document_with_generation(
        7,
        "acme chrome".to_string(),
        42,
        99,
        &[
            ("tenant".to_string(), "acme".to_string()),
            ("color".to_string(), "blue".to_string()),
            ("color".to_string(), "red".to_string()),
        ],
    );
    resident.write_to(&path).expect("write extended v2 sources");

    // A pre-ADR-116 v2 reader sees the unchanged 24-byte query index,
    // ignores the appended metadata/footer, and still recovers query text.
    let bytes = std::fs::read(&path).expect("read extended v2");
    assert_eq!(u32::from_le_bytes(bytes[4..8].try_into().unwrap()), 2);
    let old_blob_off = SRC_HEADER + SRC_IDX_REC;
    let old_query_off =
        u64::from_le_bytes(bytes[SRC_HEADER + 8..SRC_HEADER + 16].try_into().unwrap()) as usize;
    let old_query_len =
        u32::from_le_bytes(bytes[SRC_HEADER + 16..SRC_HEADER + 20].try_into().unwrap()) as usize;
    assert_eq!(
        std::str::from_utf8(
            &bytes[old_blob_off + old_query_off..old_blob_off + old_query_off + old_query_len]
        )
        .expect("old-reader query"),
        "acme chrome"
    );

    let lazy = SourceStore::open(&path, false).expect("mmap extended v2 sources");
    assert_eq!(
        lazy.get_bounded(7, 12).expect("query fits").as_deref(),
        Some("acme chrome")
    );
    let document = lazy.get_document(7).expect("stored document");
    assert_eq!(document.query(), "acme chrome");
    assert_eq!(document.version(), 42);
    assert_eq!(document.source_generation(), 99);
    assert!(document.tags_known());
    assert_eq!(
        document.tags(),
        [
            ("tenant".to_string(), "acme".to_string()),
            ("color".to_string(), "blue".to_string()),
            ("color".to_string(), "red".to_string()),
        ]
    );

    std::fs::remove_file(path).expect("remove test sources");
}

#[test]
fn source_generation_prevents_replay_from_rolling_document_backward() {
    let resident = SourceStore::new_resident();
    resident.insert_document_with_generation(7, "new".to_string(), 2, 20, &[]);
    resident.insert_document_with_generation(7, "old".to_string(), 1, 10, &[]);
    let document = resident.get_document(7).expect("resident document");
    assert_eq!(document.query(), "new");
    assert_eq!(document.source_generation(), 20);

    let path = std::env::temp_dir().join(format!(
        "reverse-rusty-monotonic-sources-{}-{}.dat",
        std::process::id(),
        NEXT_PATH.fetch_add(1, Ordering::Relaxed)
    ));
    resident.write_to(&path).expect("write lazy base");
    let lazy = SourceStore::open(&path, false).expect("open lazy base");
    lazy.insert_document_with_generation(7, "old".to_string(), 1, 10, &[]);
    assert_eq!(
        lazy.get_document(7).expect("base still wins").query(),
        "new"
    );
    lazy.insert_document_with_generation(7, "newest".to_string(), 3, 21, &[]);
    let document = lazy.get_document(7).expect("overlay document");
    assert_eq!(document.query(), "newest");
    assert_eq!(document.source_generation(), 21);

    std::fs::remove_file(path).expect("remove test sources");
}
