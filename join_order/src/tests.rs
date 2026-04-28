use super::*;
use std::collections::HashSet;

fn stats(rows: f64) -> RelStats {
    RelStats::new(rows, rows * 8.0)
}

fn problem(rel_rows: &[f64], edges: Vec<Edge>) -> Problem {
    Problem {
        rels: rel_rows.iter().copied().map(stats).collect(),
        edges,
        edge_preds: Vec::new(),
    }
}

fn unbounded_config() -> JoinOrderConfig {
    JoinOrderConfig {
        max_relations: 18,
        max_pairs: u64::MAX,
        timeout: None,
        allow_cross_joins: false,
    }
}

fn set(indices: &[RelId]) -> RelSet {
    indices.iter().fold(0, |set, rel| set | rel_bit(*rel))
}

fn brute_force_pairs(problem: &Problem) -> HashSet<(RelSet, RelSet)> {
    let root = universe(problem.rel_count());
    let connected = connected_sets(problem, root);
    let mut pairs = HashSet::new();
    for subset in 1..=root {
        if !connected[subset as usize] || is_singleton(subset) {
            continue;
        }
        for_each_split(subset, |left, right| {
            if connected[left as usize]
                && connected[right as usize]
                && problem.join_link(left, right).is_some()
            {
                pairs.insert((left, right));
            }
            Ok::<_, ()>(())
        })
        .unwrap();
    }
    pairs
}

fn counted_pairs(problem: &Problem) -> u64 {
    count_partitions(problem, unbounded_config()).unwrap().pairs
}

#[test]
fn rel_bit_builds_single_relation_sets() {
    assert_eq!(rel_bit(0), 1);
    assert_eq!(rel_bit(3), 8);
    assert_eq!(set(&[0, 2, 4]), 0b10101);
}

#[test]
fn count_matches_bruteforce_for_chain_star_clique_and_cycle() {
    let cases = [
        problem(
            &[10.0, 10.0, 10.0, 10.0],
            vec![
                Edge::inner(rel_bit(0), rel_bit(1), 0.1),
                Edge::inner(rel_bit(1), rel_bit(2), 0.1),
                Edge::inner(rel_bit(2), rel_bit(3), 0.1),
            ],
        ),
        problem(
            &[10.0, 10.0, 10.0, 10.0],
            vec![
                Edge::inner(rel_bit(0), rel_bit(1), 0.1),
                Edge::inner(rel_bit(0), rel_bit(2), 0.1),
                Edge::inner(rel_bit(0), rel_bit(3), 0.1),
            ],
        ),
        problem(
            &[10.0, 10.0, 10.0, 10.0],
            vec![
                Edge::inner(rel_bit(0), rel_bit(1), 0.1),
                Edge::inner(rel_bit(0), rel_bit(2), 0.1),
                Edge::inner(rel_bit(0), rel_bit(3), 0.1),
                Edge::inner(rel_bit(1), rel_bit(2), 0.1),
                Edge::inner(rel_bit(1), rel_bit(3), 0.1),
                Edge::inner(rel_bit(2), rel_bit(3), 0.1),
            ],
        ),
        problem(
            &[10.0, 10.0, 10.0, 10.0],
            vec![
                Edge::inner(rel_bit(0), rel_bit(1), 0.1),
                Edge::inner(rel_bit(1), rel_bit(2), 0.1),
                Edge::inner(rel_bit(2), rel_bit(3), 0.1),
                Edge::inner(rel_bit(3), rel_bit(0), 0.1),
            ],
        ),
    ];

    for case in cases {
        assert_eq!(counted_pairs(&case), brute_force_pairs(&case).len() as u64);
    }
}

#[test]
fn disconnected_graph_is_rejected_without_cross_joins() {
    let problem = problem(
        &[10.0, 10.0, 10.0, 10.0],
        vec![
            Edge::inner(rel_bit(0), rel_bit(1), 0.1),
            Edge::inner(rel_bit(2), rel_bit(3), 0.1),
        ],
    );

    assert_eq!(
        optimize(&problem, unbounded_config()).unwrap_err(),
        OptimizeError::DisconnectedGraph
    );
}

#[test]
fn cross_join_mode_can_connect_disconnected_components() {
    let problem = problem(
        &[10.0, 10.0, 10.0],
        vec![Edge::inner(rel_bit(0), rel_bit(1), 0.1)],
    );
    let mut config = unbounded_config();
    config.allow_cross_joins = true;

    let solution = optimize(&problem, config).unwrap();
    assert_eq!(solution.root(), set(&[0, 1, 2]));
}

#[test]
fn cross_join_mode_connects_three_fully_disconnected_relations() {
    let problem = problem(&[10.0, 20.0, 30.0], Vec::new());
    let mut config = unbounded_config();
    config.allow_cross_joins = true;

    let solution = optimize(&problem, config).unwrap();
    assert_eq!(solution.root(), set(&[0, 1, 2]));
    assert!(solution.best(set(&[0, 1])).is_some());
    assert!(solution.best(solution.root()).is_some());
}

#[test]
fn cross_join_mode_connects_two_disconnected_join_components() {
    let problem = problem(
        &[10.0, 10.0, 20.0, 20.0],
        vec![
            Edge::inner(rel_bit(0), rel_bit(1), 0.1),
            Edge::inner(rel_bit(2), rel_bit(3), 0.1),
        ],
    );
    let mut config = unbounded_config();
    config.allow_cross_joins = true;

    let solution = optimize(&problem, config).unwrap();
    assert_eq!(solution.root(), set(&[0, 1, 2, 3]));
    assert!(solution.best(set(&[0, 1])).is_some());
    assert!(solution.best(set(&[2, 3])).is_some());
    assert!(solution.best(solution.root()).is_some());
}

#[test]
fn fully_disconnected_graph_is_rejected_without_cross_joins() {
    let problem = problem(&[10.0, 20.0, 30.0], Vec::new());

    assert_eq!(
        optimize(&problem, unbounded_config()).unwrap_err(),
        OptimizeError::DisconnectedGraph
    );
}

#[test]
fn cross_join_budget_counts_synthetic_intermediates() {
    let problem = problem(&[10.0, 20.0, 30.0], Vec::new());
    let config = JoinOrderConfig {
        max_pairs: 1,
        allow_cross_joins: true,
        ..unbounded_config()
    };

    assert!(matches!(
        optimize(&problem, config),
        Err(OptimizeError::BudgetExceeded { .. })
    ));
}

#[test]
fn hyperedge_connects_only_after_required_side_is_available() {
    let problem = problem(
        &[10.0, 10.0, 10.0],
        vec![
            Edge::inner(rel_bit(0), rel_bit(1), 0.1),
            Edge::inner(set(&[0, 1]), rel_bit(2), 0.1),
        ],
    );

    let pairs = brute_force_pairs(&problem);
    assert!(pairs.contains(&(set(&[0, 1]), rel_bit(2))));
    assert!(!pairs.contains(&(set(&[0, 2]), rel_bit(1))));
}

#[test]
fn filtered_dimension_changes_join_order() {
    let problem = problem(
        &[1_000_000.0, 10.0, 100_000.0],
        vec![
            Edge::inner(rel_bit(0), rel_bit(1), 0.000001),
            Edge::inner(rel_bit(0), rel_bit(2), 0.000001),
        ],
    );

    let solution = optimize(&problem, unbounded_config()).unwrap();
    let root = solution.best(solution.root()).unwrap();
    assert!(
        root.left == set(&[0, 1]) || root.right == set(&[0, 1]),
        "expected filtered dimension to join with fact first, got {root:?}"
    );
}

#[test]
fn cycle_applies_all_predicates_between_two_sides() {
    let problem = problem(
        &[100.0, 100.0, 100.0],
        vec![
            Edge::inner(rel_bit(0), rel_bit(1), 0.1),
            Edge::inner(rel_bit(1), rel_bit(2), 0.1),
            Edge::inner(rel_bit(0), rel_bit(2), 0.1),
        ],
    );

    let link = problem.join_link(set(&[0, 1]), rel_bit(2)).unwrap();
    assert!((link.selectivity - 0.01).abs() < f64::EPSILON);
}

#[test]
fn budget_is_enforced_by_count_prepass() {
    let problem = problem(
        &[10.0, 10.0, 10.0, 10.0],
        vec![
            Edge::inner(rel_bit(0), rel_bit(1), 0.1),
            Edge::inner(rel_bit(0), rel_bit(2), 0.1),
            Edge::inner(rel_bit(0), rel_bit(3), 0.1),
        ],
    );
    let config = JoinOrderConfig {
        max_pairs: 1,
        ..unbounded_config()
    };

    assert!(matches!(
        optimize(&problem, config),
        Err(OptimizeError::BudgetExceeded { .. })
    ));
}

#[test]
fn predicate_ranges_are_validated() {
    let problem = Problem {
        rels: vec![stats(10.0), stats(10.0)],
        edges: vec![Edge::inner(rel_bit(0), rel_bit(1), 0.1).with_predicates(0, 1)],
        edge_preds: Vec::new(),
    };

    assert!(matches!(
        optimize(&problem, unbounded_config()),
        Err(OptimizeError::InvalidEdge { .. })
    ));
}

#[test]
fn unsupported_join_kinds_are_rejected() {
    let mut edge = Edge::inner(rel_bit(0), rel_bit(1), 0.1);
    edge.kind = JoinKind::Left;
    let problem = problem(&[10.0, 10.0], vec![edge]);

    assert!(matches!(
        optimize(&problem, unbounded_config()),
        Err(OptimizeError::UnsupportedJoinKind { .. })
    ));
}
