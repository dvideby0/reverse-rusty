//! Deterministic distributed emission ownership (ADR-109).
//!
//! Placement metadata is identity metadata, not query semantics: it never
//! participates in candidate retrieval or exact verification. Cluster reads use
//! it only after a row has matched to select the one routed shard position that
//! may emit that logical query. Standalone rows carry [`QueryPlacement::standalone`]
//! and keep their pre-ADR-109 behavior only on standalone (non-ownership) reads;
//! under an ownership-suppressed cluster read [`OwnershipContext::owner`] returns
//! `None` for them, i.e. they are never emitted — cluster ingestion paths must
//! stamp real placement.

use std::fmt;

/// Monotonic identity of the placement function used to write a cluster row.
/// Generation zero is reserved for standalone engine rows.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct PlacementGeneration(pub u64);

impl PlacementGeneration {
    pub const STANDALONE: Self = Self(0);
    pub const INITIAL: Self = Self(1);

    pub const fn get(self) -> u64 {
        self.0
    }

    pub fn next(self) -> Option<Self> {
        self.0.checked_add(1).map(Self)
    }
}

/// How a logical query is placed across shard positions.
#[repr(u8)]
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum PlacementMode {
    /// Single-node engine data; no distributed ownership suppression.
    #[default]
    Standalone = 0,
    /// The row exists only at the sorted positions stored with it.
    Selective = 1,
    /// Class-B pair placement: the row is always-visible at every position.
    ReplicatedAlwaysVisible = 2,
    /// Class-C/D placement: the row is evaluated only by the broad evaluator.
    ReplicatedBroad = 3,
}

impl PlacementMode {
    pub fn from_byte(value: u8) -> Result<Self, OwnershipError> {
        match value {
            0 => Ok(Self::Standalone),
            1 => Ok(Self::Selective),
            2 => Ok(Self::ReplicatedAlwaysVisible),
            3 => Ok(Self::ReplicatedBroad),
            other => Err(OwnershipError::UnknownMode(other)),
        }
    }
}

/// Owned write-time placement metadata carried by one logical query version.
#[derive(Clone, Debug, Default, PartialEq, Eq, Hash)]
pub struct QueryPlacement {
    generation: PlacementGeneration,
    num_shards: u32,
    mode: PlacementMode,
    positions: Vec<u32>,
}

impl QueryPlacement {
    pub fn standalone() -> Self {
        Self::default()
    }

    pub fn selective(
        generation: PlacementGeneration,
        num_shards: u32,
        mut positions: Vec<u32>,
    ) -> Result<Self, OwnershipError> {
        positions.sort_unstable();
        positions.dedup();
        let value = Self {
            generation,
            num_shards,
            mode: PlacementMode::Selective,
            positions,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn replicated_always_visible(
        generation: PlacementGeneration,
        num_shards: u32,
    ) -> Result<Self, OwnershipError> {
        Self::replicated(
            generation,
            num_shards,
            PlacementMode::ReplicatedAlwaysVisible,
        )
    }

    pub fn replicated_broad(
        generation: PlacementGeneration,
        num_shards: u32,
    ) -> Result<Self, OwnershipError> {
        Self::replicated(generation, num_shards, PlacementMode::ReplicatedBroad)
    }

    fn replicated(
        generation: PlacementGeneration,
        num_shards: u32,
        mode: PlacementMode,
    ) -> Result<Self, OwnershipError> {
        let value = Self {
            generation,
            num_shards,
            mode,
            positions: Vec::new(),
        };
        value.validate()?;
        Ok(value)
    }

    /// Construct from a persistence or transport representation without
    /// normalizing it. Non-canonical position arrays must fail loud.
    pub fn from_raw(
        generation: PlacementGeneration,
        num_shards: u32,
        mode: u8,
        positions: Vec<u32>,
    ) -> Result<Self, OwnershipError> {
        let value = Self {
            generation,
            num_shards,
            mode: PlacementMode::from_byte(mode)?,
            positions,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), OwnershipError> {
        if self.mode == PlacementMode::Standalone {
            if self.generation != PlacementGeneration::STANDALONE
                || self.num_shards != 0
                || !self.positions.is_empty()
            {
                return Err(OwnershipError::InvalidStandalone);
            }
            return Ok(());
        }
        if self.generation == PlacementGeneration::STANDALONE {
            return Err(OwnershipError::MissingGeneration);
        }
        if self.num_shards == 0 {
            return Err(OwnershipError::EmptyShardSpace);
        }
        match self.mode {
            PlacementMode::Selective => {
                if self.positions.is_empty() {
                    return Err(OwnershipError::EmptySelectivePlacement);
                }
                let mut previous = None;
                for &position in &self.positions {
                    if position >= self.num_shards {
                        return Err(OwnershipError::PositionOutOfRange {
                            position,
                            num_shards: self.num_shards,
                        });
                    }
                    if previous.is_some_and(|p| p >= position) {
                        return Err(OwnershipError::PositionsNotStrictlySorted);
                    }
                    previous = Some(position);
                }
            }
            PlacementMode::ReplicatedAlwaysVisible | PlacementMode::ReplicatedBroad => {
                if !self.positions.is_empty() {
                    return Err(OwnershipError::UnexpectedReplicatedPositions);
                }
            }
            PlacementMode::Standalone => return Err(OwnershipError::InvalidStandalone),
        }
        Ok(())
    }

    pub fn validate_for_shard(
        &self,
        position: u32,
        generation: PlacementGeneration,
        num_shards: u32,
    ) -> Result<(), OwnershipError> {
        self.validate()?;
        if self.generation != generation {
            return Err(OwnershipError::GenerationMismatch {
                expected: generation,
                actual: self.generation,
            });
        }
        if self.num_shards != num_shards {
            return Err(OwnershipError::ShardCountMismatch {
                expected: num_shards,
                actual: self.num_shards,
            });
        }
        if position >= num_shards {
            return Err(OwnershipError::PositionOutOfRange {
                position,
                num_shards,
            });
        }
        if self.mode == PlacementMode::Selective && self.positions.binary_search(&position).is_err()
        {
            return Err(OwnershipError::LocalPositionMissing(position));
        }
        Ok(())
    }

    pub fn as_ref(&self) -> QueryPlacementRef<'_> {
        QueryPlacementRef {
            generation: self.generation,
            num_shards: self.num_shards,
            mode: self.mode,
            positions: &self.positions,
        }
    }

    pub fn generation(&self) -> PlacementGeneration {
        self.generation
    }

    pub fn num_shards(&self) -> u32 {
        self.num_shards
    }

    pub fn mode(&self) -> PlacementMode {
        self.mode
    }

    pub fn positions(&self) -> &[u32] {
        &self.positions
    }
}

/// Allocation-free view over an in-memory or mmap-backed placement row.
#[derive(Clone, Copy, Debug)]
pub struct QueryPlacementRef<'a> {
    pub generation: PlacementGeneration,
    pub num_shards: u32,
    pub mode: PlacementMode,
    pub positions: &'a [u32],
}

impl QueryPlacementRef<'_> {
    pub fn to_owned(self) -> QueryPlacement {
        QueryPlacement {
            generation: self.generation,
            num_shards: self.num_shards,
            mode: self.mode,
            positions: self.positions.to_vec(),
        }
    }
}

/// Read-time routing context shared by every shard participating in one cluster
/// request. Positions must be sorted and unique so ownership is a two-pointer
/// intersection with no allocation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OwnershipContext {
    generation: PlacementGeneration,
    num_shards: u32,
    routed_positions: Vec<u32>,
    broad_evaluator: Option<u32>,
}

impl OwnershipContext {
    pub fn new(
        generation: PlacementGeneration,
        num_shards: u32,
        routed_positions: Vec<u32>,
        broad_evaluator: Option<u32>,
    ) -> Result<Self, OwnershipError> {
        let value = Self {
            generation,
            num_shards,
            routed_positions,
            broad_evaluator,
        };
        value.validate()?;
        Ok(value)
    }

    pub fn validate(&self) -> Result<(), OwnershipError> {
        if self.generation == PlacementGeneration::STANDALONE {
            return Err(OwnershipError::MissingGeneration);
        }
        if self.num_shards == 0 {
            return Err(OwnershipError::EmptyShardSpace);
        }
        if self.routed_positions.is_empty() {
            return Err(OwnershipError::EmptyRoute);
        }
        let mut previous = None;
        for &position in &self.routed_positions {
            if position >= self.num_shards {
                return Err(OwnershipError::PositionOutOfRange {
                    position,
                    num_shards: self.num_shards,
                });
            }
            if previous.is_some_and(|p| p >= position) {
                return Err(OwnershipError::RouteNotStrictlySorted);
            }
            previous = Some(position);
        }
        if let Some(position) = self.broad_evaluator {
            if self.routed_positions.binary_search(&position).is_err() {
                return Err(OwnershipError::BroadEvaluatorNotRouted(position));
            }
        }
        Ok(())
    }

    /// Fail loud when this shard's own position is absent from the routed set.
    /// `owner()` can only ever select a routed position, so an unrouted local
    /// position would silently emit nothing — a false-negative surface — instead
    /// of surfacing the mis-targeted request.
    pub fn require_routed(&self, current_position: u32) -> Result<(), OwnershipError> {
        if current_position >= self.num_shards
            || self
                .routed_positions
                .binary_search(&current_position)
                .is_err()
        {
            return Err(OwnershipError::LocalPositionMissing(current_position));
        }
        Ok(())
    }

    pub fn generation(&self) -> PlacementGeneration {
        self.generation
    }

    pub fn num_shards(&self) -> u32 {
        self.num_shards
    }

    pub fn routed_positions(&self) -> &[u32] {
        &self.routed_positions
    }

    pub fn broad_evaluator(&self) -> Option<u32> {
        self.broad_evaluator
    }

    /// Return the sole position allowed to emit `placement`, if the query is in
    /// this request's evaluated scope.
    #[inline]
    pub fn owner(&self, placement: QueryPlacementRef<'_>) -> Option<u32> {
        if placement.generation != self.generation || placement.num_shards != self.num_shards {
            return None;
        }
        match placement.mode {
            PlacementMode::Standalone => None,
            PlacementMode::Selective => {
                let (mut p, mut r) = (0usize, 0usize);
                while p < placement.positions.len() && r < self.routed_positions.len() {
                    match placement.positions[p].cmp(&self.routed_positions[r]) {
                        std::cmp::Ordering::Less => p += 1,
                        std::cmp::Ordering::Greater => r += 1,
                        std::cmp::Ordering::Equal => return Some(placement.positions[p]),
                    }
                }
                None
            }
            PlacementMode::ReplicatedAlwaysVisible => self.routed_positions.first().copied(),
            PlacementMode::ReplicatedBroad => self.broad_evaluator,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum OwnershipError {
    UnknownMode(u8),
    InvalidStandalone,
    MissingGeneration,
    EmptyShardSpace,
    EmptySelectivePlacement,
    PositionsNotStrictlySorted,
    RouteNotStrictlySorted,
    UnexpectedReplicatedPositions,
    PositionOutOfRange {
        position: u32,
        num_shards: u32,
    },
    LocalPositionMissing(u32),
    BroadEvaluatorNotRouted(u32),
    EmptyRoute,
    PlacementDecisionMismatch,
    GenerationMismatch {
        expected: PlacementGeneration,
        actual: PlacementGeneration,
    },
    ShardCountMismatch {
        expected: u32,
        actual: u32,
    },
}

impl fmt::Display for OwnershipError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "invalid distributed ownership metadata: {self:?}")
    }
}

impl std::error::Error for OwnershipError {}

/// Compile-time emission policy used by the scalar matcher. Standalone APIs use
/// [`EmitAll`]; cluster reads use [`UniqueOwner`].
pub(crate) trait EmissionPolicy: Copy {
    fn should_emit(self, placement: QueryPlacementRef<'_>) -> bool;
}

#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct EmitAll;

impl EmissionPolicy for EmitAll {
    #[inline]
    fn should_emit(self, _placement: QueryPlacementRef<'_>) -> bool {
        true
    }
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct UniqueOwner<'a> {
    context: &'a OwnershipContext,
    current_position: u32,
}

impl<'a> UniqueOwner<'a> {
    pub(crate) fn new(context: &'a OwnershipContext, current_position: u32) -> Self {
        Self {
            context,
            current_position,
        }
    }
}

impl EmissionPolicy for UniqueOwner<'_> {
    #[inline]
    fn should_emit(self, placement: QueryPlacementRef<'_>) -> bool {
        self.context.owner(placement) == Some(self.current_position)
    }
}

/// Per-title emission policy for the batch matcher (ADR-112): the batch
/// analogue of [`EmissionPolicy`], indexed by chunk-local title position.
/// `title_policy` hands the per-title scalar lanes their [`EmissionPolicy`];
/// `should_emit` is the columnar kernel's per-(title, candidate) check.
pub(crate) trait BatchEmissionPolicy: Copy {
    type TitlePolicy: EmissionPolicy;
    fn title_policy(self, title_index: usize) -> Self::TitlePolicy;
    fn should_emit(self, title_index: usize, placement: QueryPlacementRef<'_>) -> bool;
}

impl BatchEmissionPolicy for EmitAll {
    type TitlePolicy = EmitAll;
    #[inline]
    fn title_policy(self, _title_index: usize) -> EmitAll {
        EmitAll
    }
    #[inline]
    fn should_emit(self, _title_index: usize, _placement: QueryPlacementRef<'_>) -> bool {
        true
    }
}

/// One [`OwnershipContext`] per batch title, index-aligned with the title
/// chunk the kernel is evaluating. A mixed-up index would silently move a
/// logical row's emission to the wrong title's owner, so the driver slices
/// contexts and titles from the same chunk base.
#[derive(Clone, Copy, Debug)]
pub(crate) struct PerTitleUniqueOwner<'a> {
    contexts: &'a [OwnershipContext],
    current_position: u32,
}

impl<'a> PerTitleUniqueOwner<'a> {
    pub(crate) fn new(contexts: &'a [OwnershipContext], current_position: u32) -> Self {
        Self {
            contexts,
            current_position,
        }
    }
}

impl<'a> BatchEmissionPolicy for PerTitleUniqueOwner<'a> {
    type TitlePolicy = UniqueOwner<'a>;
    #[inline]
    fn title_policy(self, title_index: usize) -> UniqueOwner<'a> {
        UniqueOwner::new(&self.contexts[title_index], self.current_position)
    }
    #[inline]
    fn should_emit(self, title_index: usize, placement: QueryPlacementRef<'_>) -> bool {
        self.title_policy(title_index).should_emit(placement)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn selective_owner_is_minimum_placement_route_intersection() {
        let placement = QueryPlacement::selective(PlacementGeneration(7), 16, vec![9, 2, 5])
            .expect("valid placement");
        let context =
            OwnershipContext::new(PlacementGeneration(7), 16, vec![1, 5, 9, 12], Some(12))
                .expect("valid route");
        assert_eq!(context.owner(placement.as_ref()), Some(5));
    }

    #[test]
    fn replicated_modes_select_their_distinct_scope_owner() {
        let context = OwnershipContext::new(PlacementGeneration(3), 8, vec![2, 4, 6], Some(6))
            .expect("valid route");
        let always = QueryPlacement::replicated_always_visible(PlacementGeneration(3), 8)
            .expect("valid placement");
        let broad =
            QueryPlacement::replicated_broad(PlacementGeneration(3), 8).expect("valid placement");
        assert_eq!(context.owner(always.as_ref()), Some(2));
        assert_eq!(context.owner(broad.as_ref()), Some(6));
    }

    #[test]
    fn invalid_or_stale_metadata_fails_closed() {
        assert!(QueryPlacement::from_raw(PlacementGeneration(1), 3, 1, vec![2, 1]).is_err());
        let placement =
            QueryPlacement::selective(PlacementGeneration(2), 3, vec![1]).expect("valid placement");
        let context =
            OwnershipContext::new(PlacementGeneration(3), 3, vec![1], None).expect("valid route");
        assert_eq!(context.owner(placement.as_ref()), None);
        assert!(OwnershipContext::new(PlacementGeneration(1), 3, Vec::new(), None).is_err());
        assert!(QueryPlacement::from_raw(PlacementGeneration(1), 3, 1, vec![3]).is_err());
        assert!(QueryPlacement::from_raw(PlacementGeneration(1), 3, 2, vec![1]).is_err());
    }

    #[test]
    fn selective_owner_matches_minimum_intersection_exhaustively() {
        for num_shards in 1..=6u32 {
            let limit = 1u32 << num_shards;
            for placement_mask in 1..limit {
                let positions: Vec<u32> = (0..num_shards)
                    .filter(|position| placement_mask & (1 << position) != 0)
                    .collect();
                let placement = QueryPlacement::selective(
                    PlacementGeneration(5),
                    num_shards,
                    positions.clone(),
                )
                .expect("placement subset");
                for route_mask in 1..limit {
                    let route: Vec<u32> = (0..num_shards)
                        .filter(|position| route_mask & (1 << position) != 0)
                        .collect();
                    let context = OwnershipContext::new(
                        PlacementGeneration(5),
                        num_shards,
                        route.clone(),
                        None,
                    )
                    .expect("route subset");
                    let expected = positions
                        .iter()
                        .copied()
                        .find(|position| route.binary_search(position).is_ok());
                    assert_eq!(context.owner(placement.as_ref()), expected);
                }
            }
        }
    }
}
