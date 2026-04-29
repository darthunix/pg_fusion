# Runtime filter specs

`RuntimeFilterLifecycle.tla` models the shared-memory lifecycle used by
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

Smoke run:

```sh
java -cp "$TLA_JAR" tlc2.TLC -deadlock -cleanup -workers 1 \
  -config runtime_filter/spec/RuntimeFilterLifecycle.cfg \
  runtime_filter/spec/RuntimeFilterLifecycle.tla
```
