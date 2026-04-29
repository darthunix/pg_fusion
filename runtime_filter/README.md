# runtime_filter

Shared-memory friendly runtime filters for `pg_fusion`.

The crate is split into two layers:

- `AtomicBloomRef` is only the atomic Bloom bitset over caller-owned
  `AtomicU64` storage.
- `RuntimeFilterSlot` owns the shared-memory lifecycle around that bitset.
  Builders acquire an exclusive `Building` lease before clearing or inserting,
  publish a generation as `Ready`, or disable the same generation via CAS.
  Probes reject rows only when their expected generation is currently `Ready`;
  all stale, free, building, or disabled states pass rows unfiltered.

This keeps the filter payload reusable while avoiding false negatives from
clearing storage under old probes or letting stale builders overwrite newer
generations. A ready generation can be retired for reuse only through
`retire_ready_after_quiescence`, which is unsafe because the caller must prove
that no old probe is still reading the bitset.
