//! Demo: compile the spec's worked example, show its compiled form + chosen
//! signatures, then match a few titles and explain each pass/fail.

use reverse_rusty::compile::compile_one;
use reverse_rusty::dict::Dict;
use reverse_rusty::explain::{explain_compiled, explain_match};
use reverse_rusty::normalize::Normalizer;
use reverse_rusty::segment::{Engine, MatchScratch};

fn main() {
    let norm = Normalizer::default_vocab().expect("built-in vocab");

    // ---- 1) Compile + explain the spec example ----
    let spec_query = "1994 (upper deck,UD) michael jordan sp (preview,previews) \
        -(next,checklist,checklists,heroes,long,count) \
        -(minor,minors,top,classic,alumni) \
        -(auto,autograph,autographs,autographed,signed,dna,signature) \
        PSA 10 -(sgc,bgs)";

    let mut dict = Dict::new();
    let mut lc = String::new();
    let cq = match compile_one(spec_query, 42, 1, &norm, &mut dict, &mut lc) {
        Ok(cq) => cq,
        Err(e) => {
            eprintln!("built-in spec query failed to compile: {e}");
            std::process::exit(1);
        }
    };

    println!("===== COMPILED QUERY (spec example) =====");
    println!("{}", explain_compiled(&cq, &dict));

    println!("===== EXPLAIN: title vs query =====");
    let titles = [
        "1994 Upper Deck Michael Jordan SP Preview PSA GEM MT 10", // should PASS
        "1994 UD Michael Jordan SP Previews PSA10", // missing grade->? has psa10 => grade 10, PASS
        "1994 Upper Deck Michael Jordan SP Preview PSA 10 auto", // forbidden auto -> FAIL
        "1994 Upper Deck Michael Jordan SP Preview BGS 9.5", // wrong grader/grade -> FAIL
        "1993 Upper Deck Michael Jordan SP Preview PSA 10", // wrong year -> FAIL
    ];
    for t in titles {
        print!("{}", explain_match(&cq, t, &norm, &dict));
        println!();
    }

    // ---- 2) Same via the full Engine (candidate retrieval + verify) ----
    println!("===== ENGINE END-TO-END =====");
    let queries = vec![
        (42u64, spec_query.to_string()),
        (7u64, "michael jordan".to_string()), // broad
        (
            9u64,
            "1994 upper deck michael jordan sp psa 10 -auto".to_string(),
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
        "1994 Upper Deck Michael Jordan SP Preview PSA GEM MT 10",
        "1994 Upper Deck Michael Jordan SP Preview PSA 10 auto",
    ] {
        let st = eng.match_title(t, &mut s, &mut out, true);
        println!(
            "title {:?}\n  -> matched logical ids: {:?}  (unique candidates examined={}, postings scanned={})",
            t, out, st.unique_candidates, st.postings_scanned
        );
    }
}
