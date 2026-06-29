# filter

Shared-memory friendly runtime filters for `pg_fusion`.

`filter` provides the small concurrency boundary used by `pg_fusion`
to publish join-derived filters from workers and let PostgreSQL backends skip
probe-side rows before tuple-to-Arrow encoding.

The design goal is conservative correctness: a runtime filter may pass rows
that could have been rejected, but it must not reject a row that could join.
That means all not-yet-ready, stale, disabled, or reused filters are treated as
"pass unfiltered".

## Layers

The crate is split into three layers:

- `AtomicBloomRef` is only the atomic Bloom bitset over caller-owned
  `AtomicU64` storage. It has no ownership or lifecycle semantics.
- `RuntimeFilterSlot` owns the shared-memory lifecycle around that bitset.
  Builders acquire an exclusive `Building` lease before clearing or inserting,
  publish a generation as `Ready`, or disable the same generation via CAS.
  Probes reject rows only when their expected generation is currently `Ready`;
  all stale, free, building, or disabled states pass rows unfiltered.
- `RuntimeFilterPool` adds fixed-slot shared-memory ownership metadata and
  probe reference counts. It maps `(RuntimeFilterExecId, scan_id,
  output_column, key_type)` to a lifecycle slot and delays storage reuse until
  the owner and all probe handles are gone. Slot state, refcount, and
  publication epoch are one packed atomic word, so a probe can only acquire a
  ref for the exact slot publication it observed.

This keeps the filter payload reusable while avoiding false negatives from
clearing storage under old probes or letting stale builders overwrite newer
generations. A ready generation can be retired directly only through
`retire_ready_after_quiescence`, which is unsafe because the caller must prove
that no old probe is still reading the bitset; production shared-memory reuse
should go through `RuntimeFilterPool`.

## Lifecycle

Each filter slot stores a packed `(generation, state)` word:

- `Free`: no owner. A builder may claim the slot and advance the generation.
- `Building`: exactly one builder owns the Bloom payload. Probes must pass rows
  unfiltered because the filter can still contain false negatives.
- `Ready`: the Bloom payload is complete for the generation. Probes for that
  same generation may return `DefinitelyAbsent`.
- `Disabled`: the generation intentionally has no usable filter. Probes pass
  rows unfiltered.

The generation is part of every build and probe handle. If storage is reused,
old probe handles observe a generation mismatch and stop filtering. The pool
also keeps reference counts so storage is not cleared while an old probe could
still be reading Bloom words.

## Shared-memory pool

`RuntimeFilterPool` is intended for fixed-size shared-memory regions:

1. The postmaster computes `RuntimeFilterPool::layout(config)` and allocates a
   region with the returned size/alignment.
2. Startup code initializes the region with `RuntimeFilterPool::init_in_place`.
3. Workers attach and call `allocate_build(target)` for a specific scan target.
4. Backends attach and call `lookup_probes(exec_id, scan_id, &mut probes)`.
5. Build and probe handles release references on drop; the pool retires and
   frees a slot only after the owner and all probes have gone away.

When the pool is exhausted, callers should continue without a filter. Exhaustion
is a performance miss, not a correctness failure.

## Typical usage

```rust
use filter::{
    BloomParams, ProbeDecision, RuntimeFilterPool, RuntimeFilterPoolConfig,
    RuntimeFilterExecId, RuntimeFilterTarget, RuntimeFilterKeyType, hash_int_key,
};

# fn example(region: *mut u8, region_len: usize) -> Result<(), Box<dyn std::error::Error>> {
let params = BloomParams::new(1 << 20, 4, 0)?;
let config = RuntimeFilterPoolConfig::new(64, params);

// Startup path initializes the caller-owned shared-memory region.
let pool = unsafe { RuntimeFilterPool::init_in_place(region, region_len, config)? };

let exec_id = RuntimeFilterExecId::new(1, 2, 3, 7);
let target = RuntimeFilterTarget {
    exec_id,
    scan_id: 42,
    output_column: 3,
    key_type: RuntimeFilterKeyType::Int64,
};

if let Some(build) = pool.allocate_build(target)? {
    build.insert_hash(hash_int_key(10));
    build.publish_ready()?;
}

let mut probes = Vec::new();
pool.lookup_probes(exec_id, 42, &mut probes);
for probe in &probes {
    if probe.decision_for_hash(hash_int_key(11)) == ProbeDecision::DefinitelyAbsent {
        // The row can be skipped before expensive decode/encode work.
    }
}
# Ok(())
# }
```

## Hashing contract

The Bloom filter stores already-hashed keys. `pg_fusion` currently exposes
helpers for supported runtime-filter key families:

- `hash_bool_key` for `bool`
- `hash_int_key` for `int2` / `int4` / `int8`, `date`, `time`,
  `timestamp`, and `timestamptz`
- `hash_float32_key` for `float4`
- `hash_float64_key` for `float8`
- `hash_bytes_key` for text-like `Utf8View` keys (`text`, `varchar`,
  `bpchar`, and `name` in the extension scan schema), `bytea`, and `uuid`

Both build and probe sides must call the same helper for the same logical key
type. Float helpers normalize signed zero and NaNs so the two sides agree on
logical equality-sensitive bit patterns. Text-like keys are hashed as their
encoded UTF-8 bytes; callers must not invent ad-hoc byte encodings at call
sites.

## Correctness rules

- Never apply a filter while it is `Building`; that can create false negatives.
- Never clear/reuse Bloom storage until every old probe reference is gone.
- Never acquire a pool probe ref with separate state/refcount atomics; the
  packed publication word is the incarnation guard.
- Stale builders must not publish or disable newer generations.
- Missing, disabled, stale, or exhausted filters must pass rows unfiltered.
- Null probe values are `DefinitelyAbsent` only for a matching `Ready`
  generation because build-side join keys do not insert nulls.

## Tests and model

The crate contains deterministic unit tests and loom-style lifecycle coverage
for allocation, generation changes, stale handles, and pool reuse. The TLA+
model under `spec/` describes the lifecycle and reuse protocol at the state
machine level; update it when changing shared-memory ownership or generation
semantics.

Useful local checks:

```sh
cargo test -p filter
cargo doc -p filter --no-deps
```
