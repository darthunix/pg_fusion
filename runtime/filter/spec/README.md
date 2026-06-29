# Runtime filter specs

`RuntimeFilterLifecycle.tla` models the shared-memory Bloom lifecycle used by
opportunistic runtime filters.

The model intentionally treats Bloom bits abstractly as a set of inserted keys.
It checks the protocol property that probe-side readers never reject before the
filter is `Ready`, ignore stale generations, and never reject a key that was in
the completed build set. It also models the shared-memory pool rule that only
one builder owns a payload at a time; acquiring a new builder clears payload
only after the slot has moved from `Free` or `Disabled` to `Building` and no
probe references are active. Ready retirement is modeled as an external
quiescence boundary: the low-level API exposes it as unsafe because active
probes cannot be tracked by the Bloom bits themselves, while `RuntimeFilterPool`
tracks probe refs before reusing storage.

`RuntimeFilterPoolPublication.tla` models the pool-level publication protocol
around one shared slot. This is the layer that keeps a slot unprobeable while
`RuntimeFilterPool::allocate_build` initializes target metadata, then publishes
it with one packed `SLOT_ALLOCATED` word containing the owner ref and
publication epoch. The model checks that probes can attach only after
publication, delayed lookups cannot mutate a reused epoch, initializing slots
cannot gain probe refs, failed initialization rolls back to a clean free slot,
and retiring slots are reused only after all refs are gone.

The split is intentional: the lifecycle spec is about Bloom readiness and
false-negative safety; the publication spec is about the intermediate
multi-step pool protocol that the Rust implementation refines. Rust-side
coverage for this publication seam lives in the deterministic
`initializing_pool_slot_is_invisible_to_probe_lookup` unit test and the
`pool_initializing_state_blocks_probe_refcount_race` and
`stale_observed_probe_cannot_mutate_reused_pool_slot` loom tests.

SANY checks:

```sh
~/script/tla/tla sany runtime/filter/spec/RuntimeFilterLifecycle.tla
~/script/tla/tla sany runtime/filter/spec/RuntimeFilterPoolPublication.tla
```

Smoke runs:

```sh
~/script/tla/tla tlc -deadlock -cleanup -workers 1 \
  -config runtime/filter/spec/RuntimeFilterLifecycle.cfg \
  runtime/filter/spec/RuntimeFilterLifecycle.tla

~/script/tla/tla tlc -deadlock -cleanup -workers 1 \
  -config runtime/filter/spec/RuntimeFilterPoolPublication.cfg \
  runtime/filter/spec/RuntimeFilterPoolPublication.tla
```
