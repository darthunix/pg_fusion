//! Compact join-order optimizer core.
//!
//! `join_order` intentionally knows nothing about SQL engines. Callers provide
//! filtered relation statistics, join edges, and opaque predicate handles. The
//! optimizer returns a dynamic-programming table that can be used to rebuild an
//! engine-specific join tree.

use std::error::Error;
use std::fmt;
use std::time::{Duration, Instant};

/// Relation id inside one join-ordering problem.
pub type RelId = u8;

/// Bitset of relations.
pub type RelSet = u64;

/// Opaque predicate handle owned by the caller.
pub type PredId = u32;

const MAX_U64_RELS: usize = 64;
const TIMEOUT_CHECK_INTERVAL: u64 = 128;

/// Return the one-bit set for `rel`.
pub const fn rel_bit(rel: RelId) -> RelSet {
    1u64 << rel
}

fn universe(rel_count: usize) -> RelSet {
    if rel_count == MAX_U64_RELS {
        RelSet::MAX
    } else {
        (1u64 << rel_count) - 1
    }
}

fn lowest_bit(set: RelSet) -> RelSet {
    set & set.wrapping_neg()
}

fn is_singleton(set: RelSet) -> bool {
    set != 0 && (set & (set - 1)) == 0
}

fn width(stats: PlanStats) -> f64 {
    if stats.rows > 0.0 {
        stats.bytes / stats.rows
    } else {
        0.0
    }
}

/// Filtered relation statistics supplied by the caller.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct RelStats {
    pub rows: f64,
    pub bytes: f64,
}

impl RelStats {
    pub const fn new(rows: f64, bytes: f64) -> Self {
        Self { rows, bytes }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[repr(u8)]
pub enum JoinKind {
    Inner = 0,
    Left = 1,
    Right = 2,
    Full = 3,
    Semi = 4,
    Anti = 5,
    Cross = 6,
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EdgeFlags(u8);

impl EdgeFlags {
    pub const COMMUTATIVE: Self = Self(1 << 0);

    pub const fn empty() -> Self {
        Self(0)
    }

    pub const fn contains(self, other: Self) -> bool {
        (self.0 & other.0) == other.0
    }

    pub const fn union(self, other: Self) -> Self {
        Self(self.0 | other.0)
    }
}

/// Join-hypergraph edge.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Edge {
    pub left: RelSet,
    pub right: RelSet,
    pub pred_start: u32,
    pub pred_len: u16,
    pub kind: JoinKind,
    pub flags: EdgeFlags,
    pub selectivity: f64,
}

impl Edge {
    pub const fn inner(left: RelSet, right: RelSet, selectivity: f64) -> Self {
        Self {
            left,
            right,
            pred_start: 0,
            pred_len: 0,
            kind: JoinKind::Inner,
            flags: EdgeFlags::COMMUTATIVE,
            selectivity,
        }
    }

    pub const fn with_predicates(mut self, pred_start: u32, pred_len: u16) -> Self {
        self.pred_start = pred_start;
        self.pred_len = pred_len;
        self
    }
}

/// Optimizer input.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct Problem {
    pub rels: Vec<RelStats>,
    pub edges: Vec<Edge>,
    pub edge_preds: Vec<PredId>,
}

impl Problem {
    pub fn rel_count(&self) -> usize {
        self.rels.len()
    }
}

/// Optimizer configuration.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct JoinOrderConfig {
    pub max_relations: usize,
    pub max_pairs: u64,
    pub timeout: Option<Duration>,
    pub allow_cross_joins: bool,
}

impl Default for JoinOrderConfig {
    fn default() -> Self {
        Self {
            max_relations: 18,
            max_pairs: 1_000_000,
            timeout: Some(Duration::from_millis(100)),
            allow_cross_joins: false,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum OptimizeError {
    EmptyProblem,
    TooManyRelations {
        rel_count: usize,
        max_relations: usize,
    },
    InvalidRelSet {
        set: RelSet,
        universe: RelSet,
    },
    InvalidEdge {
        edge_index: usize,
        reason: &'static str,
    },
    UnsupportedJoinKind {
        edge_index: usize,
        kind: JoinKind,
    },
    DisconnectedGraph,
    BudgetExceeded {
        pairs: u64,
        max_pairs: u64,
    },
    Timeout {
        pairs: u64,
    },
    NoPlan,
}

impl fmt::Display for OptimizeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyProblem => write!(f, "join-order problem has no relations"),
            Self::TooManyRelations {
                rel_count,
                max_relations,
            } => write!(
                f,
                "join-order problem has {rel_count} relations, max configured is {max_relations}"
            ),
            Self::InvalidRelSet { set, universe } => {
                write!(f, "relation set {set:#x} is outside universe {universe:#x}")
            }
            Self::InvalidEdge { edge_index, reason } => {
                write!(f, "invalid join edge {edge_index}: {reason}")
            }
            Self::UnsupportedJoinKind { edge_index, kind } => {
                write!(f, "unsupported join kind {kind:?} on edge {edge_index}")
            }
            Self::DisconnectedGraph => write!(f, "join graph is disconnected"),
            Self::BudgetExceeded { pairs, max_pairs } => write!(
                f,
                "join-order search budget exceeded after {pairs} connected partition pairs; max is {max_pairs}"
            ),
            Self::Timeout { pairs } => {
                write!(f, "join-order search timed out after {pairs} connected partition pairs")
            }
            Self::NoPlan => write!(f, "join-order optimizer did not find a valid plan"),
        }
    }
}

impl Error for OptimizeError {}

/// Count-only prepass result.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CountResult {
    pub rel_count: usize,
    pub pairs: u64,
    pub exceeded: bool,
}

/// Build side chosen for a binary join.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BuildSide {
    Left,
    Right,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Best {
    pub cost: f64,
    pub rows: f64,
    pub bytes: f64,
    pub left: RelSet,
    pub right: RelSet,
    pub build_side: BuildSide,
}

#[derive(Clone, Debug, PartialEq)]
pub struct Solution {
    root: RelSet,
    entries: Vec<Option<Best>>,
}

impl Solution {
    pub fn root(&self) -> RelSet {
        self.root
    }

    pub fn best(&self, set: RelSet) -> Option<&Best> {
        self.entries.get(set as usize)?.as_ref()
    }

    pub fn entries(&self) -> &[Option<Best>] {
        &self.entries
    }
}

#[derive(Clone, Copy, Debug)]
struct PlanStats {
    cost: f64,
    rows: f64,
    bytes: f64,
}

#[derive(Clone, Copy, Debug)]
struct JoinLink {
    selectivity: f64,
    commutative: bool,
}

struct SearchCtx {
    started_at: Instant,
    timeout: Option<Duration>,
    pairs: u64,
    max_pairs: u64,
}

impl SearchCtx {
    fn new(config: JoinOrderConfig) -> Self {
        Self {
            started_at: Instant::now(),
            timeout: config.timeout,
            pairs: 0,
            max_pairs: config.max_pairs,
        }
    }

    fn observe_pair(&mut self) -> Result<(), OptimizeError> {
        self.pairs = self.pairs.saturating_add(1);
        if self.pairs > self.max_pairs {
            return Err(OptimizeError::BudgetExceeded {
                pairs: self.pairs,
                max_pairs: self.max_pairs,
            });
        }
        if self.pairs % TIMEOUT_CHECK_INTERVAL == 0 {
            if let Some(timeout) = self.timeout {
                if self.started_at.elapsed() >= timeout {
                    return Err(OptimizeError::Timeout { pairs: self.pairs });
                }
            }
        }
        Ok(())
    }
}

/// Count connected partition pairs without costing them.
pub fn count_partitions(
    problem: &Problem,
    config: JoinOrderConfig,
) -> Result<CountResult, OptimizeError> {
    validate(problem, config)?;

    let rel_count = problem.rel_count();
    let root = universe(rel_count);
    let mut ctx = SearchCtx::new(config);
    let connected = connected_sets(problem, root);

    let mut reachable = vec![false; (root as usize) + 1];
    for rel_id in 0..rel_count {
        reachable[1usize << rel_id] = true;
    }

    for width in 2..=rel_count {
        for set in 1..=root {
            if set.count_ones() as usize != width {
                continue;
            }
            if !config.allow_cross_joins && !connected[set as usize] {
                continue;
            }

            let mut set_reachable = false;
            for_each_split(set, |left, right| {
                let sides_ready = if config.allow_cross_joins {
                    reachable[left as usize] && reachable[right as usize]
                } else {
                    connected[left as usize] && connected[right as usize]
                };
                if !sides_ready {
                    return Ok(());
                }
                if problem.join_link(left, right).is_some() || config.allow_cross_joins {
                    ctx.observe_pair()?;
                    set_reachable = true;
                }
                Ok(())
            })?;
            reachable[set as usize] = set_reachable;
        }
    }

    Ok(CountResult {
        rel_count,
        pairs: ctx.pairs,
        exceeded: false,
    })
}

/// Optimize one connected join component.
pub fn optimize(problem: &Problem, config: JoinOrderConfig) -> Result<Solution, OptimizeError> {
    validate(problem, config)?;

    let prepass = match count_partitions(problem, config) {
        Ok(result) => result,
        Err(OptimizeError::BudgetExceeded { pairs, .. }) => {
            return Err(OptimizeError::BudgetExceeded {
                pairs,
                max_pairs: config.max_pairs,
            });
        }
        Err(error) => return Err(error),
    };
    if prepass.exceeded {
        return Err(OptimizeError::BudgetExceeded {
            pairs: prepass.pairs,
            max_pairs: config.max_pairs,
        });
    }

    let rel_count = problem.rel_count();
    let root = universe(rel_count);
    let connected = connected_sets(problem, root);
    if !connected[root as usize] && !config.allow_cross_joins {
        return Err(OptimizeError::DisconnectedGraph);
    }

    let mut entries = vec![None; (root as usize) + 1];
    for rel_id in 0..rel_count {
        let set = 1u64 << rel_id;
        let stats = problem.rels[rel_id];
        entries[set as usize] = Some(Best {
            cost: 0.0,
            rows: stats.rows,
            bytes: stats.bytes,
            left: 0,
            right: 0,
            build_side: BuildSide::Right,
        });
    }

    let mut ctx = SearchCtx::new(config);

    for width in 2..=rel_count {
        for set in 1..=root {
            if set.count_ones() as usize != width {
                continue;
            }
            if !connected[set as usize] && !config.allow_cross_joins {
                continue;
            }

            let mut best: Option<Best> = None;
            for_each_split(set, |left, right| {
                let sides_ready = if config.allow_cross_joins {
                    entries[left as usize].is_some() && entries[right as usize].is_some()
                } else {
                    connected[left as usize] && connected[right as usize]
                };
                if !sides_ready {
                    return Ok(());
                }

                let link = problem.join_link(left, right).or_else(|| {
                    config.allow_cross_joins.then_some(JoinLink {
                        selectivity: 1.0,
                        commutative: true,
                    })
                });
                let Some(link) = link else {
                    return Ok(());
                };
                ctx.observe_pair()?;

                let Some(left_stats) = entries[left as usize].map(plan_stats) else {
                    return Ok(());
                };
                let Some(right_stats) = entries[right as usize].map(plan_stats) else {
                    return Ok(());
                };
                let candidate = estimate_join(left, right, left_stats, right_stats, link);
                if best
                    .map(|current| candidate.cost < current.cost)
                    .unwrap_or(true)
                {
                    best = Some(candidate);
                }
                Ok(())
            })?;

            entries[set as usize] = best;
        }
    }

    if entries[root as usize].is_none() {
        return Err(OptimizeError::NoPlan);
    }

    Ok(Solution { root, entries })
}

fn plan_stats(best: Best) -> PlanStats {
    PlanStats {
        cost: best.cost,
        rows: best.rows,
        bytes: best.bytes,
    }
}

fn estimate_join(
    left: RelSet,
    right: RelSet,
    left_stats: PlanStats,
    right_stats: PlanStats,
    link: JoinLink,
) -> Best {
    let rows = (left_stats.rows * right_stats.rows * link.selectivity).max(0.0);
    let bytes = rows * (width(left_stats) + width(right_stats));
    let left_build_cost = left_stats.bytes + right_stats.rows;
    let right_build_cost = right_stats.bytes + left_stats.rows;
    let (build_side, join_work) = if link.commutative && right_build_cost < left_build_cost {
        (BuildSide::Right, right_build_cost)
    } else {
        (BuildSide::Left, left_build_cost)
    };
    let cost = left_stats.cost + right_stats.cost + join_work + rows + bytes;
    Best {
        cost,
        rows,
        bytes,
        left,
        right,
        build_side,
    }
}

fn for_each_split<E>(
    set: RelSet,
    mut f: impl FnMut(RelSet, RelSet) -> Result<(), E>,
) -> Result<(), E> {
    let anchor = lowest_bit(set);
    let mut left = (set - 1) & set;
    while left != 0 {
        if (left & anchor) != 0 && left != set {
            let right = set ^ left;
            if right != 0 {
                f(left, right)?;
            }
        }
        left = (left - 1) & set;
    }
    Ok(())
}

fn validate(problem: &Problem, config: JoinOrderConfig) -> Result<(), OptimizeError> {
    let rel_count = problem.rel_count();
    if rel_count == 0 {
        return Err(OptimizeError::EmptyProblem);
    }
    if rel_count > MAX_U64_RELS || rel_count > config.max_relations {
        return Err(OptimizeError::TooManyRelations {
            rel_count,
            max_relations: config.max_relations.min(MAX_U64_RELS),
        });
    }
    if rel_count >= usize::BITS as usize {
        return Err(OptimizeError::TooManyRelations {
            rel_count,
            max_relations: (usize::BITS as usize) - 1,
        });
    }
    let universe = universe(rel_count);
    for (rel_index, stats) in problem.rels.iter().enumerate() {
        if !stats.rows.is_finite() || stats.rows < 0.0 {
            return Err(OptimizeError::InvalidEdge {
                edge_index: rel_index,
                reason: "relation rows must be finite and non-negative",
            });
        }
        if !stats.bytes.is_finite() || stats.bytes < 0.0 {
            return Err(OptimizeError::InvalidEdge {
                edge_index: rel_index,
                reason: "relation bytes must be finite and non-negative",
            });
        }
    }
    for (edge_index, edge) in problem.edges.iter().enumerate() {
        if matches!(
            edge.kind,
            JoinKind::Left | JoinKind::Right | JoinKind::Full | JoinKind::Semi | JoinKind::Anti
        ) {
            return Err(OptimizeError::UnsupportedJoinKind {
                edge_index,
                kind: edge.kind,
            });
        }
        if edge.left == 0 || edge.right == 0 {
            return Err(OptimizeError::InvalidEdge {
                edge_index,
                reason: "edge sides must be non-empty",
            });
        }
        if (edge.left & edge.right) != 0 {
            return Err(OptimizeError::InvalidEdge {
                edge_index,
                reason: "edge sides must be disjoint",
            });
        }
        if (edge.left | edge.right) & !universe != 0 {
            return Err(OptimizeError::InvalidRelSet {
                set: edge.left | edge.right,
                universe,
            });
        }
        let pred_end = edge.pred_start as usize + edge.pred_len as usize;
        if pred_end > problem.edge_preds.len() {
            return Err(OptimizeError::InvalidEdge {
                edge_index,
                reason: "predicate range is outside edge_preds",
            });
        }
        if !edge.selectivity.is_finite() || edge.selectivity < 0.0 {
            return Err(OptimizeError::InvalidEdge {
                edge_index,
                reason: "edge selectivity must be finite and non-negative",
            });
        }
    }
    Ok(())
}

fn connected_sets(problem: &Problem, root: RelSet) -> Vec<bool> {
    let mut connected = vec![false; (root as usize) + 1];
    for set in 1..=root {
        connected[set as usize] = is_connected(problem, set);
    }
    connected
}

fn is_connected(problem: &Problem, set: RelSet) -> bool {
    if is_singleton(set) {
        return true;
    }
    let mut visited = lowest_bit(set);
    loop {
        let before = visited;
        for edge in &problem.edges {
            if !matches!(edge.kind, JoinKind::Inner | JoinKind::Cross) {
                continue;
            }
            let edge_set = edge.left | edge.right;
            if edge_set & !set != 0 {
                continue;
            }
            if edge_set & visited != 0 {
                visited |= edge_set;
            }
        }
        if visited == before {
            break;
        }
    }
    visited == set
}

impl Problem {
    fn join_link(&self, left: RelSet, right: RelSet) -> Option<JoinLink> {
        let mut selectivity = 1.0;
        let mut found = false;
        let mut commutative = true;

        for edge in &self.edges {
            if !matches!(edge.kind, JoinKind::Inner | JoinKind::Cross) {
                continue;
            }
            let forward = edge.left & !left == 0 && edge.right & !right == 0;
            let reverse = edge.left & !right == 0 && edge.right & !left == 0;
            if forward || reverse {
                found = true;
                selectivity *= edge.selectivity;
                commutative &= edge.flags.contains(EdgeFlags::COMMUTATIVE);
            }
        }

        found.then_some(JoinLink {
            selectivity,
            commutative,
        })
    }
}

#[cfg(test)]
mod tests;
