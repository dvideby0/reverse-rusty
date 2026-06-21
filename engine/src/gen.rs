//! Synthetic data generator — deterministic, seeded, adversarial.
//!
//! Design: — (standalone tooling, no dedicated design doc)
//! Invariant: Deterministic (SplitMix64 PRNG) — same seed = same data every time
//! Hot path: no — used by benchmarks and oracle tests only
//!
//! Generates trading-card product queries and listing titles. Models the
//! adversarial cases from the spec: hot-entity skew, configurable broad-query
//! fraction, near-duplicate query families, alternate title forms (PSA10 vs
//! PSA 10, UD vs Upper Deck), and forbidden-term noise.

/// Tiny deterministic PRNG (SplitMix64). No external crates.
pub struct Rng(u64);
impl Rng {
    pub fn new(seed: u64) -> Self {
        Rng(seed.wrapping_add(0x9e37_79b9_7f4a_7c15))
    }
    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }
    #[inline]
    pub fn below(&mut self, n: usize) -> usize {
        (self.next_u64() % (n as u64)) as usize
    }
    #[inline]
    pub fn frac(&mut self) -> f64 {
        (self.next_u64() >> 11) as f64 / (1u64 << 53) as f64
    }
    /// Zipf-ish skewed index into 0..n: lower indices much more likely.
    #[inline]
    pub fn skewed(&mut self, n: usize, skew: f64) -> usize {
        let u = self.frac().max(1e-12);
        let x = u.powf(skew); // skew>1 pushes toward 0
        ((x * n as f64) as usize).min(n - 1)
    }
    /// Pick a random index in `0..slice.len()`, or `None` for an empty slice —
    /// `below` would divide by zero on a 0-length pool. The non-empty path
    /// consumes the RNG exactly as a bare `below(slice.len())`, so default
    /// (non-degenerate) generation stays byte-identical.
    #[inline]
    fn pick<'a, T>(&mut self, slice: &'a [T]) -> Option<&'a T> {
        if slice.is_empty() {
            None
        } else {
            Some(&slice[self.below(slice.len())])
        }
    }
    /// Skew-pick from a slice, or `None` for an empty slice — `skewed` underflows
    /// (`n - 1` on `n == 0`) on an empty pool. Non-empty path is byte-identical
    /// to `skewed(slice.len(), skew)`.
    #[inline]
    fn pick_skewed<'a, T>(&mut self, slice: &'a [T], skew: f64) -> Option<&'a T> {
        if slice.is_empty() {
            None
        } else {
            Some(&slice[self.skewed(slice.len(), skew)])
        }
    }
}

pub const PLAYERS: &[&str] = &[
    "michael jordan",
    "lebron james",
    "kobe bryant",
    "tom brady",
    "ken griffey",
    "wayne gretzky",
    "patrick mahomes",
    "mike trout",
];
// alternate forms the title may use for the same entity
pub const BRANDS: &[&str] = &[
    "upper deck",
    "topps",
    "panini",
    "fleer",
    "donruss",
    "bowman",
    "score",
    "prizm",
];
pub const BRAND_ALT: &[&str] = &[
    "ud", "topps", "panini", "fleer", "donruss", "bowman", "score", "prizm",
];
pub const CARD_TERMS: &[&str] = &[
    "sp",
    "rc",
    "rookie",
    "refractor",
    "insert",
    "base",
    "preview",
];
pub const GRADERS: &[&str] = &["psa", "bgs", "sgc"];
pub const GRADES: &[&str] = &["8", "9", "9.5", "10"];
pub const NEGATIVES: &[&str] = &["auto", "signed", "reprint", "lot", "checklist", "minor"];
pub const NOISE: &[&str] = &[
    "card", "nm", "sharp", "centered", "hof", "vintage", "graded", "slab", "pop", "rare",
];

#[derive(Clone)]
pub struct GenConfig {
    pub num_queries: usize,
    pub num_titles: usize,
    pub broad_query_frac: f64,
    pub hot_skew: f64,      // >1 = more skew toward popular players/graders
    pub family_size: usize, // near-duplicate variants per family base
    pub seed: u64,
    /// Size of the synthetic entity space. Real marketplaces have millions of
    /// distinct players/sets; a large pool here makes selectivity realistic
    /// instead of an artifact of a tiny vocabulary.
    pub num_players: usize,
    pub num_sets: usize,
}

impl Default for GenConfig {
    fn default() -> Self {
        GenConfig {
            num_queries: 1_000_000,
            num_titles: 20_000,
            broad_query_frac: 0.05,
            hot_skew: 2.0,
            family_size: 8,
            seed: 0x00C0_FFEE,
            num_players: 20_000,
            num_sets: 8_000,
        }
    }
}

pub struct Dataset {
    pub queries: Vec<(u64, String)>,
    pub titles: Vec<String>,
}

/// Build the entity pools (real anchors + large synthetic space).
struct Pools {
    players: Vec<String>,
    sets: Vec<String>,
}
fn build_pools(cfg: &GenConfig) -> Pools {
    let mut players: Vec<String> = PLAYERS
        .iter()
        .map(std::string::ToString::to_string)
        .collect();
    for i in 0..cfg.num_players {
        // single-token synthetic player; normalizes to a distinct generic feature
        players.push(format!("athlete{i:05}"));
    }
    let mut sets = Vec::with_capacity(cfg.num_sets);
    for i in 0..cfg.num_sets {
        sets.push(format!("series{i:04}"));
    }
    Pools { players, sets }
}

pub fn generate(cfg: &GenConfig) -> Dataset {
    let mut rng = Rng::new(cfg.seed);
    let pools = build_pools(cfg);
    let queries = gen_queries(&mut rng, cfg, &pools);
    let titles = gen_titles(&mut rng, cfg, &pools);
    Dataset { queries, titles }
}

fn gen_queries(rng: &mut Rng, cfg: &GenConfig, pools: &Pools) -> Vec<(u64, String)> {
    let mut out = Vec::with_capacity(cfg.num_queries);
    let mut id: u64 = 0;
    while out.len() < cfg.num_queries {
        if rng.frac() < cfg.broad_query_frac {
            // broad query: just a (hot) player, or a grade, or a card term
            let q = match rng.below(3) {
                // Empty player pool -> fall back to a grade so we never index an
                // empty vec (degenerate config only; default pool is non-empty).
                0 => rng.pick_skewed(&pools.players, cfg.hot_skew).map_or_else(
                    || format!("{} {}", GRADERS[0], GRADES[GRADES.len() - 1]),
                    Clone::clone,
                ),
                1 => format!("{} {}", GRADERS[0], GRADES[GRADES.len() - 1]), // "psa 10"
                _ => "rookie".to_string(),
            };
            out.push((id, q));
            id += 1;
            continue;
        }
        // a near-duplicate family sharing player+year+brand+set. `pools.players`
        // always carries the built-in PLAYERS; `pools.sets` is empty iff
        // `num_sets == 0`, so skip the set token rather than index an empty pool.
        let Some(player) = rng.pick_skewed(&pools.players, cfg.hot_skew) else {
            // No players to anchor a family on (degenerate config): skip.
            id += 1;
            continue;
        };
        let year = 1986 + rng.below(39); // 1986..2024
        let brand = BRANDS[rng.below(BRANDS.len())];
        let set = rng.pick(&pools.sets).map(String::as_str);
        let fam = cfg.family_size.max(1);
        for _ in 0..fam {
            if out.len() >= cfg.num_queries {
                break;
            }
            let mut q = match set {
                Some(set) => format!("{year} {brand} {set} {player}"),
                None => format!("{year} {brand} {player}"),
            };
            // card term
            if rng.frac() < 0.7 {
                q.push(' ');
                q.push_str(CARD_TERMS[rng.below(CARD_TERMS.len())]);
            }
            // grader + grade
            if rng.frac() < 0.6 {
                let g = GRADERS[rng.skewed(GRADERS.len(), cfg.hot_skew)];
                let gr = GRADES[rng.skewed(GRADES.len(), cfg.hot_skew)];
                q.push_str(&format!(" {g} {gr}"));
            }
            // negatives (0..3)
            let nn = rng.below(4);
            for _ in 0..nn {
                q.push_str(&format!(" -{}", NEGATIVES[rng.below(NEGATIVES.len())]));
            }
            out.push((id, q));
            id += 1;
        }
    }
    out.truncate(cfg.num_queries);
    out
}

/// Generate `n` negation-only (cost class D) query texts — 1–3 forbidden terms,
/// no positives: the "base/raw card defined entirely by exclusions" shape the
/// ADR-068 always-candidate lane exists for. Seeded + deterministic.
///
/// A SEPARATE function rather than a `GenConfig` field, the same opt-in-by-
/// construction discipline as messy mode (ADR-063): every existing benchmark /
/// oracle corpus stays byte-identical. Callers (the class-D differential) assign
/// logical ids and ingest under `accept_class_d`; a default engine rejects these.
pub fn gen_class_d_queries(seed: u64, n: usize) -> Vec<String> {
    let mut rng = Rng::new(seed);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let nn = 1 + rng.below(3); // 1..=3 forbidden terms
        let mut q = String::new();
        for _ in 0..nn {
            if !q.is_empty() {
                q.push(' ');
            }
            q.push_str(&format!("-{}", NEGATIVES[rng.below(NEGATIVES.len())]));
        }
        out.push(q);
    }
    out
}

// ---- Adversarial surface noise ("messy mode") ----------------------------------------
//
// The clean generator above produces lowercase, single-spaced, punctuation-free ASCII —
// the *easiest possible* surface for the normalizer. Real listing titles are not like
// that, and several historical escaped bugs (whitespace runs, boundary-invalid phrase
// matches, punctuation handling) lived exactly in the gap. These helpers wrap any clean
// string in deterministic, seeded surface noise so the differential oracle and the
// metamorphic suites run over adversarial bytes too.
//
// Opt-in by construction (separate functions, no `GenConfig` field), so every existing
// benchmark / oracle corpus stays byte-identical.

/// Foldable diacritic substitutions — each folds back to its base letter in
/// `normalize::fold_diacritic`, so sprinkling them is surface noise, not a semantic change.
const DIACRITICS: &[(char, char)] = &[
    ('a', 'á'),
    ('e', 'é'),
    ('i', 'î'),
    ('o', 'ö'),
    ('u', 'ü'),
    ('n', 'ñ'),
    ('c', 'ç'),
    ('s', 'š'),
    ('z', 'ž'),
];

/// Unicode junk tokens a real marketplace title can carry: trademark signs, emoji, CJK,
/// soup that should normalize to nothing (or to a synthetic out-of-dict feature) without
/// panicking or perturbing unrelated matches.
const UNICODE_JUNK: &[&str] = &["™", "®", "🔥🔥", "カード", "новый", "★★★", "½"];

/// Apply random capitalization: the cleaner lowercases everything, so case is pure noise.
fn mess_case(rng: &mut Rng, s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphabetic() && rng.frac() < 0.4 {
                c.to_ascii_uppercase()
            } else {
                c
            }
        })
        .collect()
}

/// Replace some foldable base letters with their diacritic forms.
fn mess_diacritics(rng: &mut Rng, s: &str) -> String {
    s.chars()
        .map(|c| {
            if rng.frac() < 0.15 {
                DIACRITICS
                    .iter()
                    .find(|&&(base, _)| base == c)
                    .map_or(c, |&(_, d)| d)
            } else {
                c
            }
        })
        .collect()
}

/// Widen some inter-token gaps into whitespace runs (spaces/tabs) and pad the ends.
fn mess_whitespace(rng: &mut Rng, s: &str) -> String {
    let mut out = String::with_capacity(s.len() + 8);
    if rng.frac() < 0.3 {
        out.push_str("  ");
    }
    for ch in s.chars() {
        if ch == ' ' && rng.frac() < 0.4 {
            out.push_str(if rng.frac() < 0.2 { " \t " } else { "  " });
        } else {
            out.push(ch);
        }
    }
    if rng.frac() < 0.3 {
        out.push(' ');
    }
    out
}

/// Title-only punctuation noise: trailing bangs, commas between tokens, a parenthesized
/// token, an apostrophe or hyphen spliced *inside* a token (splits it under the default
/// punctuation table — the ground truth moves with it, consistently on both sides).
fn mess_punct_title(rng: &mut Rng, s: &str) -> String {
    let toks: Vec<&str> = s.split_whitespace().collect();
    let mut out: Vec<String> = Vec::with_capacity(toks.len());
    for t in toks {
        let mut t = t.to_string();
        match rng.below(12) {
            0 => t.push('!'),
            1 => t.push_str("!!"),
            2 => t.push(','),
            3 => t = format!("({t})"),
            4 if t.len() > 2 => {
                let mid = t.len() / 2;
                if t.is_char_boundary(mid) {
                    let c = if rng.frac() < 0.5 { '\'' } else { '-' };
                    t.insert(mid, c);
                }
            }
            _ => {}
        }
        out.push(t);
    }
    out.join(" ")
}

/// Append assorted junk tokens: unicode soup, long out-of-dict tokens, a duplicated token.
fn mess_extra_tokens(rng: &mut Rng, s: &str) -> String {
    let mut out = s.to_string();
    if rng.frac() < 0.5 {
        out.push(' ');
        out.push_str(UNICODE_JUNK[rng.below(UNICODE_JUNK.len())]);
    }
    if rng.frac() < 0.5 {
        out.push_str(&format!(" zzoov{:016x}", rng.next_u64()));
    }
    if rng.frac() < 0.4 {
        let toks: Vec<&str> = s.split_whitespace().collect();
        if !toks.is_empty() {
            out.push(' ');
            out.push_str(toks[rng.below(toks.len())]);
        }
    }
    out
}

/// Surface-mess a **title**: any byte sequence is legal on the title side, so this
/// composes case noise, diacritics, whitespace runs, punctuation, and junk tokens.
/// Rarely (~1%) it instead pads the title with dozens of distinct out-of-dict tokens —
/// a >64-distinct-feature title that stresses the verifier's non-mask tail.
pub fn messify_title(rng: &mut Rng, title: &str) -> String {
    if rng.frac() < 0.01 {
        let mut t = title.to_string();
        for i in 0..80 {
            t.push_str(&format!(" pad{i}x{:08x}", rng.next_u64() as u32));
        }
        return t;
    }
    let mut t = title.to_string();
    if rng.frac() < 0.6 {
        t = mess_case(rng, &t);
    }
    if rng.frac() < 0.4 {
        t = mess_diacritics(rng, &t);
    }
    if rng.frac() < 0.5 {
        t = mess_punct_title(rng, &t);
    }
    if rng.frac() < 0.6 {
        t = mess_whitespace(rng, &t);
    }
    if rng.frac() < 0.4 {
        t = mess_extra_tokens(rng, &t);
    }
    t
}

/// Surface-mess a **query** while keeping it DSL-parseable and structurally identical:
/// per-token case/diacritic noise and whitespace runs between clauses. Tokens carrying
/// DSL structure (a `-` negation prefix, quotes, parens) are left untouched, so the
/// clause shape — and therefore the query's semantics under the shared normalizer —
/// is preserved exactly.
pub fn messify_query(rng: &mut Rng, query: &str) -> String {
    let toks: Vec<&str> = query.split_whitespace().collect();
    let mut out: Vec<String> = Vec::with_capacity(toks.len());
    for t in toks {
        let structural = t.starts_with('-')
            || t.contains('"')
            || t.contains('(')
            || t.contains(')')
            || t.contains(',');
        if structural {
            out.push(t.to_string());
            continue;
        }
        let mut t = t.to_string();
        if rng.frac() < 0.5 {
            t = mess_case(rng, &t);
        }
        if rng.frac() < 0.3 {
            t = mess_diacritics(rng, &t);
        }
        out.push(t);
    }
    let mut joined = String::with_capacity(query.len() + 8);
    for (i, t) in out.iter().enumerate() {
        if i > 0 {
            joined.push_str(if rng.frac() < 0.25 { "  " } else { " " });
        }
        joined.push_str(t);
    }
    joined
}

/// Mess a fraction of an existing clean dataset in place (titles and, more conservatively,
/// queries). Deterministic for a given `rng` state; `title_frac`/`query_frac` are the
/// probabilities that any one string is perturbed at all.
pub fn messify_dataset(rng: &mut Rng, data: &mut Dataset, title_frac: f64, query_frac: f64) {
    for t in &mut data.titles {
        if rng.frac() < title_frac {
            *t = messify_title(rng, t);
        }
    }
    for (_, q) in &mut data.queries {
        if rng.frac() < query_frac {
            *q = messify_query(rng, q);
        }
    }
}

fn gen_titles(rng: &mut Rng, cfg: &GenConfig, pools: &Pools) -> Vec<String> {
    let mut out = Vec::with_capacity(cfg.num_titles);
    for _ in 0..cfg.num_titles {
        // `pools.players` always carries the built-in PLAYERS; an empty pool here
        // would mean PLAYERS itself is empty (it never is), but guard anyway so a
        // degenerate pool skips the title rather than indexing an empty vec.
        let Some(player) = rng.pick_skewed(&pools.players, cfg.hot_skew) else {
            continue;
        };
        let year = 1986 + rng.below(39);
        let bi = rng.below(BRANDS.len());
        // alternate brand form sometimes (UD vs Upper Deck)
        let brand = if rng.frac() < 0.3 {
            BRAND_ALT[bi]
        } else {
            BRANDS[bi]
        };
        // `pools.sets` is empty iff `num_sets == 0`: drop the set token instead.
        let set = rng.pick(&pools.sets).map(String::as_str);

        let mut t = match set {
            Some(set) => format!("{year} {brand} {set} {player}"),
            None => format!("{year} {brand} {player}"),
        };
        // leading/trailing noise
        if rng.frac() < 0.5 {
            t = format!("{} {}", NOISE[rng.below(NOISE.len())], t);
        }
        if rng.frac() < 0.8 {
            t.push(' ');
            t.push_str(CARD_TERMS[rng.below(CARD_TERMS.len())]);
        }
        if rng.frac() < 0.7 {
            let g = GRADERS[rng.skewed(GRADERS.len(), cfg.hot_skew)];
            let gr = GRADES[rng.skewed(GRADES.len(), cfg.hot_skew)];
            // alternate grade form: "psa 10" vs "psa10" vs "psa gem mt 10"
            match rng.below(3) {
                0 => t.push_str(&format!(" {g} {gr}")),
                1 => t.push_str(&format!(" {g}{gr}")),
                _ => t.push_str(&format!(" {g} gem mt {gr}")),
            }
        }
        // sometimes inject a term that some queries forbid (auto/lot/...)
        if rng.frac() < 0.25 {
            t.push(' ');
            t.push_str(NEGATIVES[rng.below(NEGATIVES.len())]);
        }
        if rng.frac() < 0.4 {
            t.push(' ');
            t.push_str(NOISE[rng.below(NOISE.len())]);
        }
        out.push(t);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Degenerate pools (`num_sets == 0`, `num_players == 0`) must not panic:
    /// `below(0)` divides by zero and `skewed(0, _)` underflows `n - 1`. The
    /// guards skip the missing token instead. (The default `num_players` path
    /// still has the built-in PLAYERS, so titles/queries are still produced.)
    #[test]
    fn empty_pools_do_not_panic() {
        let cfg = GenConfig {
            num_queries: 64,
            num_titles: 64,
            num_sets: 0,
            num_players: 0,
            ..GenConfig::default()
        };
        let data = generate(&cfg);
        // PLAYERS is non-empty, so generation still yields content with no sets.
        assert!(
            !data.queries.is_empty(),
            "queries still produced without sets"
        );
        assert!(
            !data.titles.is_empty(),
            "titles still produced without sets"
        );
    }

    /// Only the sets pool is empty: the common degenerate case the guards target.
    #[test]
    fn empty_set_pool_does_not_panic() {
        let cfg = GenConfig {
            num_queries: 64,
            num_titles: 64,
            num_sets: 0,
            num_players: 50,
            ..GenConfig::default()
        };
        let data = generate(&cfg);
        assert_eq!(data.queries.len(), 64);
        assert_eq!(data.titles.len(), 64);
    }
}
