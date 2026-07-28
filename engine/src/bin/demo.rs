//! Demo: compile a worked product example, show its compiled form + chosen
//! signatures, then match a few titles and explain each pass/fail.

use reverse_rusty::compile::compile_one;
use reverse_rusty::dict::Dict;
use reverse_rusty::explain::{explain_compiled, explain_match};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};

fn main() {
    let norm = Normalizer::default_vocab().expect("default vocabulary");

    // ---- 1) Compile + explain a representative product query ----
    let product_query = "2024 (north star,northstar) wireless mouse \
        (compact,portable) pro -(damaged,parts,replica,manual)";

    let mut dict = Dict::new();
    let mut lc = String::new();
    let cq = match compile_one(product_query, 42, 1, &norm, &mut dict, &mut lc, 0) {
        Ok(cq) => cq,
        Err(e) => {
            eprintln!("product query failed to compile: {e}");
            std::process::exit(1);
        }
    };

    println!("===== COMPILED PRODUCT QUERY =====");
    println!("{}", explain_compiled(&cq, &dict));

    println!("===== EXPLAIN: title vs query =====");
    let titles = [
        "2024 North Star Wireless Mouse Compact Pro", // should PASS
        "2024 Northstar Wireless Mouse Portable Pro", // alternate brand + attribute, PASS
        "2024 North Star Wireless Mouse Compact Pro damaged", // forbidden, FAIL
        "2024 North Star Wireless Mouse Compact Basic", // wrong variant, FAIL
        "2023 North Star Wireless Mouse Compact Pro", // wrong year, FAIL
    ];
    for t in titles {
        print!("{}", explain_match(&cq, t, &norm, &dict));
        println!();
    }

    // ---- 2) Same via the full Engine (candidate retrieval + verify) ----
    println!("===== ENGINE END-TO-END =====");
    let queries = vec![
        (42u64, product_query.to_string()),
        (7u64, "wireless mouse".to_string()), // broad
        (
            9u64,
            "2024 north star wireless mouse compact pro -damaged".to_string(),
        ),
    ];
    let mut eng = Engine::new(norm);
    eng.build_from_queries(&queries);
    let cc = eng.class_counts();
    println!(
        "indexed: {} queries (A={} B={} C={} D-rejected={}), dict features={}",
        eng.num_queries(),
        cc[0],
        cc[1],
        cc[2],
        cc[3],
        eng.dict_len()
    );

    let mut s = MatchScratch::new();
    let mut out = Vec::new();
    for t in [
        "2024 North Star Wireless Mouse Compact Pro",
        "2024 North Star Wireless Mouse Compact Pro damaged",
    ] {
        let st = eng.match_title(t, &mut s, &mut out, true);
        println!(
            "title {:?}\n  -> matched logical ids: {:?}  (unique candidates examined={}, postings scanned={})",
            t, out, st.unique_candidates, st.postings_scanned
        );
    }
}
