use super::{fmt_bytes, render_segments_table, SegmentRow};
use reverse_rusty::events::{SegmentInfo, SegmentKind};

fn info(ordinal: usize, kind: SegmentKind, alive: usize, deleted: usize) -> SegmentInfo {
    let entries = alive + deleted;
    SegmentInfo {
        ordinal,
        kind,
        entries,
        alive,
        deleted,
        holes_ratio: if entries == 0 {
            0.0
        } else {
            deleted as f64 / entries as f64
        },
        vocab_epoch: 3,
        stale: false,
        resident_bytes: 0,
        overhead_bytes: 0,
    }
}

#[test]
fn fmt_bytes_scales_by_unit() {
    assert_eq!(fmt_bytes(0), "0 B");
    assert_eq!(fmt_bytes(512), "512 B");
    assert_eq!(fmt_bytes(1024), "1.00 KB");
    assert_eq!(fmt_bytes(1_572_864), "1.50 MB");
    assert_eq!(fmt_bytes(3 * 1024 * 1024 * 1024), "3.00 GB");
}

#[test]
fn table_has_header_and_one_row_per_segment() {
    let infos = vec![
        info(0, SegmentKind::Mmap, 98_000, 2_000),
        info(1, SegmentKind::Memory, 50_000, 0),
        info(2, SegmentKind::Memtable, 1_200, 0),
    ];
    let table = render_segments_table(&infos);
    let lines: Vec<&str> = table.lines().collect();
    // 1 header + 3 data rows.
    assert_eq!(lines.len(), 4, "table:\n{table}");
    assert!(lines[0].contains("segment") && lines[0].contains("holes"));
    assert!(lines[1].contains("mmap"));
    assert!(lines[2].contains("memory"));
    assert!(lines[3].contains("memtable"));
    // 2000/100000 = 2.00% holes on the first base segment.
    assert!(lines[1].contains("2.00%"), "row:\n{}", lines[1]);
}

#[test]
fn stale_flag_renders_yes_no() {
    let mut stale = info(0, SegmentKind::Memory, 10, 0);
    stale.stale = true;
    let table = render_segments_table(&[stale]);
    let row = table.lines().nth(1).expect("data row");
    assert!(row.contains("yes"), "row: {row}");

    let fresh = info(0, SegmentKind::Memory, 10, 0);
    let table = render_segments_table(&[fresh]);
    let row = table.lines().nth(1).expect("data row");
    assert!(row.contains(" no "), "row: {row}");
}

#[test]
fn json_row_projects_segment_info() {
    let mut s = info(2, SegmentKind::Memtable, 1_200, 0);
    s.resident_bytes = 145_000;
    s.overhead_bytes = 18_000;
    let row = SegmentRow::from(&s);
    let json = serde_json::to_value(&row).expect("serialize");
    assert_eq!(json["kind"], "memtable");
    assert_eq!(json["ordinal"], 2);
    assert_eq!(json["alive"], 1_200);
    // Byte fields are raw integers in JSON (humanized only in the text table).
    assert_eq!(json["resident_bytes"], 145_000);
    assert_eq!(json["overhead_bytes"], 18_000);
}
