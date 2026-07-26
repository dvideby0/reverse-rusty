use super::*;

#[test]
fn batch_equals_scalar_under_tag_filter_including_materialized_pure_anchors() {
    // A high broad fraction so the columnar broad lane (and its pure-anchor
    // materialization fast path) is well exercised.
    let data = gen(0x00F1_17E5, 24_000, 2_500, 0.18);
    let eng = build_single_tagged(&data);

    let filters: [Vec<(String, Vec<String>)>; 3] = [
        vec![("category".to_string(), vec!["cards".to_string()])],
        vec![(
            "category".to_string(),
            vec!["cards".to_string(), "coins".to_string()],
        )],
        // a value never ingested ⇒ ∅ on both paths
        vec![("category".to_string(), vec!["nonexistent".to_string()])],
    ];

    let mut saw_nonempty = false;
    for filter in &filters {
        // `materialize` on AND off — `true` drives the pure-anchor fast path that the
        // Step-5 fix had to teach to honor the filter.
        for &materialize in &[true, false] {
            let scalar = scalar_filtered(&eng, &data.titles, filter);
            let batch = batch_filtered(
                &eng,
                &data.titles,
                BatchMatchOptions {
                    include_broad: true,
                    broad_batch_size: 128,
                    broad_strategy: BroadStrategy::Columnar,
                    broad_materialize: materialize,
                    broad_prefilter: true,
                },
                filter,
            );
            assert_eq!(
                scalar, batch,
                "batch ≠ scalar under filter {filter:?} (materialize={materialize})"
            );
            if scalar.iter().any(|r| !r.is_empty()) {
                saw_nonempty = true;
            }
        }
    }
    assert!(saw_nonempty, "degenerate: no filter matched anything");
}
