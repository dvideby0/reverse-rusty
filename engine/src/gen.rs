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
        Rng(seed.wrapping_add(0x9e3779b97f4a7c15))
    }
    #[inline]
    pub fn next_u64(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e3779b97f4a7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d049bb133111eb);
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
pub const CARD_TERMS: &[&str] = &["sp", "rc", "rookie", "refractor", "insert", "base", "preview"];
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
    pub hot_skew: f64, // >1 = more skew toward popular players/graders
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
            seed: 0xC0FFEE,
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
    let mut players: Vec<String> = PLAYERS.iter().map(|s| s.to_string()).collect();
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
                0 => pools.players[rng.skewed(pools.players.len(), cfg.hot_skew)].clone(),
                1 => format!("{} {}", GRADERS[0], GRADES[GRADES.len() - 1]), // "psa 10"
                _ => "rookie".to_string(),
            };
            out.push((id, q));
            id += 1;
            continue;
        }
        // a near-duplicate family sharing player+year+brand+set
        let player = &pools.players[rng.skewed(pools.players.len(), cfg.hot_skew)];
        let year = 1986 + rng.below(39); // 1986..2024
        let brand = BRANDS[rng.below(BRANDS.len())];
        let set = &pools.sets[rng.below(pools.sets.len())];
        let fam = cfg.family_size.max(1);
        for _ in 0..fam {
            if out.len() >= cfg.num_queries {
                break;
            }
            let mut q = format!("{year} {brand} {set} {player}");
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

fn gen_titles(rng: &mut Rng, cfg: &GenConfig, pools: &Pools) -> Vec<String> {
    let mut out = Vec::with_capacity(cfg.num_titles);
    for _ in 0..cfg.num_titles {
        let pi = rng.skewed(pools.players.len(), cfg.hot_skew);
        let player = &pools.players[pi];
        let year = 1986 + rng.below(39);
        let bi = rng.below(BRANDS.len());
        // alternate brand form sometimes (UD vs Upper Deck)
        let brand = if rng.frac() < 0.3 { BRAND_ALT[bi] } else { BRANDS[bi] };
        let set = &pools.sets[rng.below(pools.sets.len())];

        let mut t = format!("{year} {brand} {set} {player}");
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
