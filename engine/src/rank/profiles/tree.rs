use serde::Deserialize;

use super::{RankFeature, RankFeatureView, RankProfileError, MAX_TREE_DEPTH};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct TreeConfig {
    pub(super) nodes: Vec<TreeNodeConfig>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum TreeNodeConfig {
    Split {
        feature: RankFeature,
        threshold: i64,
        left: usize,
        right: usize,
    },
    Leaf {
        value: i64,
    },
}

#[derive(Clone, Debug)]
pub(crate) struct CompiledTree {
    pub(super) nodes: Vec<TreeNodeConfig>,
    pub(super) max_depth: usize,
}

impl CompiledTree {
    pub(super) fn compile(
        name: &str,
        tree_index: usize,
        tree: TreeConfig,
    ) -> Result<Self, RankProfileError> {
        if tree.nodes.is_empty() {
            return Err(RankProfileError::Invalid(format!(
                "profile `{name}` tree {tree_index} is empty"
            )));
        }
        let len = tree.nodes.len();
        let mut parents = vec![0u8; len];
        for node in &tree.nodes {
            if let TreeNodeConfig::Split { left, right, .. } = node {
                for child in [*left, *right] {
                    let Some(parent_count) = parents.get_mut(child) else {
                        return Err(RankProfileError::Invalid(format!(
                            "profile `{name}` tree {tree_index} references missing node {child}"
                        )));
                    };
                    *parent_count = parent_count.saturating_add(1);
                    if *parent_count > 1 {
                        return Err(RankProfileError::Invalid(format!(
                            "profile `{name}` tree {tree_index} references node {child} more than \
                             once"
                        )));
                    }
                }
            }
        }
        if parents[0] != 0 {
            return Err(RankProfileError::Invalid(format!(
                "profile `{name}` tree {tree_index} references its root as a child"
            )));
        }
        let mut state = vec![0u8; len];
        let mut stack = vec![(0usize, false, 1usize)];
        let mut max_depth = 0usize;
        while let Some((index, exiting, depth)) = stack.pop() {
            if depth > MAX_TREE_DEPTH {
                return Err(RankProfileError::Invalid(format!(
                    "profile `{name}` tree {tree_index} exceeds maximum depth {MAX_TREE_DEPTH}"
                )));
            }
            let Some(node) = tree.nodes.get(index) else {
                return Err(RankProfileError::Invalid(format!(
                    "profile `{name}` tree {tree_index} references missing node {index}"
                )));
            };
            if exiting {
                state[index] = 2;
                continue;
            }
            if state[index] == 1 {
                return Err(RankProfileError::Invalid(format!(
                    "profile `{name}` tree {tree_index} contains a cycle at node {index}"
                )));
            }
            if state[index] == 2 {
                continue;
            }
            max_depth = max_depth.max(depth);
            state[index] = 1;
            stack.push((index, true, depth));
            if let TreeNodeConfig::Split { left, right, .. } = node {
                stack.push((*right, false, depth.saturating_add(1)));
                stack.push((*left, false, depth.saturating_add(1)));
            }
        }
        if let Some(unreachable) = state.iter().position(|status| *status == 0) {
            return Err(RankProfileError::Invalid(format!(
                "profile `{name}` tree {tree_index} has unreachable node {unreachable}"
            )));
        }
        Ok(Self {
            nodes: tree.nodes,
            max_depth,
        })
    }

    #[inline]
    pub(super) fn score(&self, features: RankFeatureView) -> i64 {
        let mut index = 0usize;
        for _ in 0..self.nodes.len() {
            match self.nodes.get(index) {
                Some(TreeNodeConfig::Leaf { value }) => return *value,
                Some(TreeNodeConfig::Split {
                    feature,
                    threshold,
                    left,
                    right,
                }) => {
                    index = if features.value(*feature) <= *threshold {
                        *left
                    } else {
                        *right
                    };
                }
                None => return 0,
            }
        }
        0
    }
}
