//! A torn log tail drops only the torn record; acknowledged writes survive — and the
//! fsync policy is invisible to recovery.

use crate::harness::*;

#[test]
fn torn_log_tail_recovers_acknowledged_writes() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);
    let dir = unique_dir("torn");

    {
        let cluster = ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), true), &queries)
            .expect("durable cluster builds");
        apply_churn(&cluster, &added, &removed);
    }
    // Corrupt the tail: append junk that cannot frame a valid record.
    {
        use std::io::Write as _;
        let mut f = std::fs::OpenOptions::new()
            .append(true)
            .open(dir.join("cluster.log"))
            .expect("open log");
        f.write_all(&[0xFF, 0xFF, 0xFF, 0x7F, 0x01, 0x02, 0x03])
            .expect("corrupt");
    }

    let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
    let brute = Brute::build(&final_live(&queries, &added, &removed));
    let mut lc = String::new();
    let mut feats: Vec<u32> = Vec::new();
    for t in &titles {
        let want = brute.matches(t, &mut lc, &mut feats);
        let got: HashSet<u64> = reopened
            .percolate(t)
            .expect("percolate")
            .into_iter()
            .collect();
        assert_eq!(got, want, "torn-tail reopen {t:?}");
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ---- fsync policy is invisible to recovery ----

#[test]
fn fsync_policy_does_not_change_recovery() {
    let (queries, titles) = build_corpus();
    let (added, removed) = churn(&queries);
    for &fsync in &[false, true] {
        let dir = unique_dir(&format!("fsync_{fsync}"));
        {
            let cluster =
                ClusterEngine::build(vocab(), &durable_cfg(3, dir.clone(), fsync), &queries)
                    .expect("durable cluster builds");
            apply_churn(&cluster, &added, &removed);
        }
        let reopened = ClusterEngine::open(dir.clone(), vocab(), None).expect("reopen");
        let brute = Brute::build(&final_live(&queries, &added, &removed));
        let mut lc = String::new();
        let mut feats: Vec<u32> = Vec::new();
        for t in &titles {
            let want = brute.matches(t, &mut lc, &mut feats);
            let got: HashSet<u64> = reopened
                .percolate(t)
                .expect("percolate")
                .into_iter()
                .collect();
            assert_eq!(got, want, "fsync={fsync} {t:?}");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
