use super::*;

#[test]
fn hot_segments_write_the_v5_rollback_fence() {
    use reverse_rusty::storage::MmapSegment;
    // Sorted by filename: readdir order is FILESYSTEM-dependent (APFS returned
    // the first-written file, ext4's hash order did not — a CI-only failure), so
    // pick the first segment deterministically.
    let seg_path = |dir: &std::path::Path| -> std::path::PathBuf {
        let mut segs: Vec<std::path::PathBuf> = std::fs::read_dir(dir.join("segments"))
            .expect("read segments dir")
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| p.extension().is_some_and(|x| x == "seg"))
            .collect();
        segs.sort();
        segs.into_iter().next().expect("a sealed segment file")
    };
    let seg_version = |dir: &std::path::Path| -> u32 {
        let bytes = std::fs::read(seg_path(dir)).expect("read segment");
        u32::from_le_bytes(bytes[4..8].try_into().expect("version word"))
    };
    let manifest_version = |dir: &std::path::Path| -> u32 {
        let bytes = std::fs::read(dir.join("manifest.bin")).expect("manifest");
        u32::from_le_bytes(bytes[4..8].try_into().expect("version word"))
    };
    let queries = tiny_hot_corpus();

    // ---- hot-bearing: at least .seg/manifest v5 + the recorded θ ----
    let dir_hot = tempdir("fence-hot");
    {
        let mut cfg = cfg_theta(2);
        cfg.data_dir = Some(dir_hot.clone());
        let mut eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("open");
        eng.build_from_queries(&queries);
        assert!(eng.class_counts()[4] > 0, "degenerate: no class H");
        eng.flush();
    }
    assert!(
        seg_version(&dir_hot) >= 5,
        "hot segment must carry at least the v5 fence"
    );
    assert!(
        manifest_version(&dir_hot) >= 5,
        "hot commit must carry at least manifest v5"
    );
    assert!(
        MmapSegment::open(&seg_path(&dir_hot))
            .expect("open hot segment")
            .carries_hot_fence(),
        "the segment content must carry the hot fence"
    );
    let m = reverse_rusty::storage::read_manifest(&dir_hot.join("manifest.bin"))
        .expect("read manifest");
    assert!(
        m.hot_fence,
        "the manifest reads back with the hot fence set"
    );
    assert_eq!(m.hot_anchor_theta, 2, "the recorded θ round-trips");

    // ---- hot-free under the SAME θ knob: no content-derived hot fence ----
    let dir_plain = tempdir("fence-plain");
    {
        let mut cfg = cfg_theta(1_000_000); // nothing reaches θ
        cfg.data_dir = Some(dir_plain.clone());
        let mut eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("open");
        eng.build_from_queries(&queries);
        assert_eq!(eng.class_counts()[4], 0);
        eng.flush();
    }
    assert!(
        !MmapSegment::open(&seg_path(&dir_plain))
            .expect("open hot-free segment")
            .carries_hot_fence(),
        "hot-free segment must not report the content-derived fence"
    );

    // ---- the version ladder: hot outranks class D ----
    // The class-D query rides the BUILD corpus so ONE base segment holds both
    // classes — the ladder decision (`has_hot` checked before `has_class_d`)
    // is what this leg pins. (An insert_live + flush would seal the D query
    // into its own v4 segment and leave the ladder unexercised.)
    let dir_both = tempdir("fence-both");
    {
        let mut cfg = cfg_theta(2);
        cfg.accept_class_d = true;
        cfg.data_dir = Some(dir_both.clone());
        let mut eng = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg).expect("open");
        let mut both_queries = queries.clone();
        both_queries.push((900_000, "-manual".to_string()));
        eng.build_from_queries(&both_queries);
        let cc = eng.class_counts();
        assert!(
            cc[3] > 0 && cc[4] > 0,
            "degenerate: the ladder segment must hold BOTH class D and class H"
        );
    }
    assert!(
        seg_version(&dir_both) >= 5,
        "hot + class D must carry at least the v5 ladder fence"
    );
    assert!(manifest_version(&dir_both) >= 5);

    // ---- forged class bytes fail loud at open (never mis-bucketed) ----
    // A class byte above the version's ceiling: 5 in a v5 file…
    let forge = |src: &std::path::Path, dst_name: &str, mutate: &dyn Fn(&mut Vec<u8>)| {
        let mut bytes = std::fs::read(src).expect("read");
        mutate(&mut bytes);
        let body = bytes.len() - 4;
        let crc = reverse_rusty::storage::crc32(&bytes[..body]);
        bytes[body..].copy_from_slice(&crc.to_le_bytes());
        let dst = src.parent().expect("dir").join(dst_name);
        std::fs::write(&dst, &bytes).expect("write");
        dst
    };
    // The class array lives in the meta section: [count: u32][class bytes…],
    // located by the header's meta_off word (bytes 48..56) — forge INSIDE it.
    let forge_class = |bytes: &mut Vec<u8>, from: u8, to: u8| {
        let meta_off = u64::from_le_bytes(bytes[48..56].try_into().expect("meta_off")) as usize;
        let count =
            u32::from_le_bytes(bytes[meta_off..meta_off + 4].try_into().expect("count")) as usize;
        let arr = meta_off + 4;
        let pos = bytes[arr..arr + count]
            .iter()
            .position(|&b| b == from)
            .map(|p| p + arr)
            .expect("a class byte to forge");
        bytes[pos] = to;
    };
    let hot_seg = seg_path(&dir_hot);
    let forged5 = forge(&hot_seg, "forged5.seg", &|bytes: &mut Vec<u8>| {
        forge_class(bytes, 4, 5);
    });
    let err = MmapSegment::open(&forged5).expect_err("class byte 5 must fail loud");
    assert!(err.to_string().contains("cost-class byte"), "got: {err}");
    // …and a class byte 4 smuggled into a file declared as v3 (whose ceiling is
    // 3). The source file is a modern cumulative format, so forge the declared
    // version down as part of the corruption.
    let plain_seg = seg_path(&dir_plain);
    let forged4 = forge(&plain_seg, "forged4.seg", &|bytes: &mut Vec<u8>| {
        bytes[4..8].copy_from_slice(&3u32.to_le_bytes());
        forge_class(bytes, 0, 4);
    });
    let err = MmapSegment::open(&forged4).expect_err("class byte 4 in v3 must fail loud");
    assert!(err.to_string().contains("cost-class byte"), "got: {err}");

    // ---- an unknown FUTURE manifest version fails Engine::open outright ----
    let mpath = dir_hot.join("manifest.bin");
    let mut mbytes = std::fs::read(&mpath).expect("manifest");
    mbytes[4..8].copy_from_slice(&9u32.to_le_bytes());
    let body = mbytes.len() - 4;
    let crc = reverse_rusty::storage::crc32(&mbytes[..body]);
    mbytes[body..].copy_from_slice(&crc.to_le_bytes());
    std::fs::write(&mpath, &mbytes).expect("write");
    let mut cfg = cfg_theta(2);
    cfg.data_dir = Some(dir_hot.clone());
    let err = Engine::open(Normalizer::default_vocab().expect("vocab"), cfg)
        .expect_err("future manifest version must refuse to open");
    assert!(
        err.to_string().contains("unsupported manifest version"),
        "got: {err}"
    );

    for d in [dir_hot, dir_plain, dir_both] {
        std::fs::remove_dir_all(&d).ok();
    }
}
