//! Title introspection: run real titles through the normalizer and print the
//! features it extracts. Reveals what the current (hand-vocab) normalizer
//! catches and — more importantly — what it misses on real eBay data.
//!
//! Usage: norm <file-with-one-title-per-line>

use percolator::dict::Dict;
use percolator::normalize::Normalizer;

fn main() {
    let path = std::env::args().nth(1).expect("usage: norm <titles.txt>");
    let text = std::fs::read_to_string(&path).expect("read titles file");
    let norm = Normalizer::default_vocab().expect("built-in vocab");

    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        // compile_features interns into a throwaway dict and returns ids;
        // we re-derive the names for display.
        let mut dict = Dict::new();
        let mut lc = String::new();
        let ids = norm.compile_features(line, &mut dict, &mut lc);
        let names: Vec<&str> = ids.iter().map(|&id| dict.name(id)).collect();
        println!("TITLE: {line}");
        println!("  FEATURES: {}", names.join(", "));
        println!();
    }
}
