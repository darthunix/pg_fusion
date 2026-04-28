# join_order

`join_order` is a standalone join-order optimizer core. It has no dependency on
DataFusion, PostgreSQL, pgrx, or pg_fusion runtime crates.

The crate operates on a compact join-hypergraph:

- relations are `RelId` values stored in a `RelSet` bitset
- join predicates are opaque `PredId` handles owned by the caller
- relation statistics are already filtered by local scan predicates
- edge selectivity is already estimated by the caller

This keeps the hot optimizer path limited to numbers, bitsets, and indices. A
caller such as `plan_builder` is responsible for translating a logical plan into
`Problem`, calling `optimize`, and rebuilding the engine-specific plan from the
returned `Solution`.

## Example

```rust
use join_order::{
    optimize, Edge, JoinOrderConfig, Problem, RelStats, rel_bit,
};

let problem = Problem {
    rels: vec![
        RelStats::new(1_000_000.0, 32_000_000.0),
        RelStats::new(100.0, 3_200.0),
        RelStats::new(1_000.0, 32_000.0),
    ],
    edges: vec![
        Edge::inner(rel_bit(0), rel_bit(1), 0.000001),
        Edge::inner(rel_bit(0), rel_bit(2), 0.000001),
    ],
    edge_preds: Vec::new(),
};

let solution = optimize(&problem, JoinOrderConfig::default()).unwrap();
assert_eq!(solution.root(), rel_bit(0) | rel_bit(1) | rel_bit(2));
```

## Benchmarks

The crate has a Criterion benchmark for deterministic join-graph shapes:
chains, stars, cliques, cycles, and cross-join-enabled disconnected graphs.
It measures both the count-only prepass and full optimization.

Build the benchmark without running it:

```sh
cargo bench -p join_order --bench optimizer --no-run
```

Run the benchmark:

```sh
cargo bench -p join_order --bench optimizer
```
