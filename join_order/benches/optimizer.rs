use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion};
use join_order::{count_partitions, optimize, rel_bit, Edge, JoinOrderConfig, Problem, RelStats};

#[derive(Clone)]
struct Fixture {
    name: String,
    problem: Problem,
    config: JoinOrderConfig,
}

fn bench_config(allow_cross_joins: bool) -> JoinOrderConfig {
    JoinOrderConfig {
        max_relations: 18,
        max_pairs: u64::MAX,
        timeout: None,
        allow_cross_joins,
    }
}

fn relation_stats(rel_count: usize) -> Vec<RelStats> {
    (0..rel_count)
        .map(|index| {
            let rows = 10_000.0 + (index as f64 * 1_000.0);
            let width = 24.0 + ((index % 5) as f64 * 8.0);
            RelStats::new(rows, rows * width)
        })
        .collect()
}

fn bit(index: usize) -> u64 {
    rel_bit(index as u8)
}

fn edge_selectivity(index: usize) -> f64 {
    1.0 / (1_000.0 + index as f64)
}

fn fixture(name: impl Into<String>, rel_count: usize, edges: Vec<Edge>, cross: bool) -> Fixture {
    Fixture {
        name: name.into(),
        problem: Problem {
            rels: relation_stats(rel_count),
            edges,
            edge_preds: Vec::new(),
        },
        config: bench_config(cross),
    }
}

fn chain_fixture(rel_count: usize) -> Fixture {
    let edges = (0..rel_count - 1)
        .map(|index| Edge::inner(bit(index), bit(index + 1), edge_selectivity(index)))
        .collect();
    fixture(format!("chain_{rel_count}"), rel_count, edges, false)
}

fn star_fixture(rel_count: usize) -> Fixture {
    let edges = (1..rel_count)
        .map(|index| Edge::inner(bit(0), bit(index), edge_selectivity(index)))
        .collect();
    fixture(format!("star_{rel_count}"), rel_count, edges, false)
}

fn clique_fixture(rel_count: usize) -> Fixture {
    let mut edges = Vec::new();
    for left in 0..rel_count {
        for right in left + 1..rel_count {
            edges.push(Edge::inner(
                bit(left),
                bit(right),
                edge_selectivity(edges.len()),
            ));
        }
    }
    fixture(format!("clique_{rel_count}"), rel_count, edges, false)
}

fn cycle_fixture(rel_count: usize) -> Fixture {
    let mut edges = (0..rel_count - 1)
        .map(|index| Edge::inner(bit(index), bit(index + 1), edge_selectivity(index)))
        .collect::<Vec<_>>();
    edges.push(Edge::inner(
        bit(rel_count - 1),
        bit(0),
        edge_selectivity(rel_count),
    ));
    fixture(format!("cycle_{rel_count}"), rel_count, edges, false)
}

fn disconnected_cross_fixture(rel_count: usize) -> Fixture {
    fixture(
        format!("disconnected_cross_{rel_count}"),
        rel_count,
        Vec::new(),
        true,
    )
}

fn two_components_cross_fixture(rel_count: usize) -> Fixture {
    let split = rel_count / 2;
    let mut edges = Vec::new();
    for index in 0..split - 1 {
        edges.push(Edge::inner(
            bit(index),
            bit(index + 1),
            edge_selectivity(edges.len()),
        ));
    }
    for index in split..rel_count - 1 {
        edges.push(Edge::inner(
            bit(index),
            bit(index + 1),
            edge_selectivity(edges.len()),
        ));
    }
    fixture(
        format!("two_components_cross_{rel_count}"),
        rel_count,
        edges,
        true,
    )
}

fn fixtures() -> Vec<Fixture> {
    let mut fixtures = Vec::new();
    for rel_count in [6, 10, 14] {
        fixtures.push(chain_fixture(rel_count));
        fixtures.push(star_fixture(rel_count));
        fixtures.push(cycle_fixture(rel_count));
    }
    for rel_count in [5, 7, 9] {
        fixtures.push(clique_fixture(rel_count));
    }
    for rel_count in [4, 6, 8] {
        fixtures.push(disconnected_cross_fixture(rel_count));
        fixtures.push(two_components_cross_fixture(rel_count));
    }
    fixtures
}

fn bench_count_partitions(c: &mut Criterion) {
    let fixtures = fixtures();
    let mut group = c.benchmark_group("count_partitions");
    for fixture in &fixtures {
        group.bench_with_input(
            BenchmarkId::from_parameter(fixture.name.as_str()),
            fixture,
            |b, fixture| {
                b.iter(|| {
                    let result =
                        count_partitions(black_box(&fixture.problem), black_box(fixture.config))
                            .expect("count partitions");
                    black_box(result.pairs);
                });
            },
        );
    }
    group.finish();
}

fn bench_optimize(c: &mut Criterion) {
    let fixtures = fixtures();
    let mut group = c.benchmark_group("optimize");
    for fixture in &fixtures {
        group.bench_with_input(
            BenchmarkId::from_parameter(fixture.name.as_str()),
            fixture,
            |b, fixture| {
                b.iter(|| {
                    let solution = optimize(black_box(&fixture.problem), black_box(fixture.config))
                        .expect("optimize");
                    black_box(solution.root());
                });
            },
        );
    }
    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(20);
    targets = bench_count_partitions, bench_optimize
}
criterion_main!(benches);
