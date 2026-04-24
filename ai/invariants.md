---
id: inv-project-0001
type: invariant
scope: project
tags: ["safety", "pgrx", "ipc", "datafusion", "shared-memory", "arrow"]
updated_at: "2026-04-24"
importance: 0.95
---

# Project Invariants

1. No panics in PostgreSQL extension paths.

- In `pg/extension` and backend-facing pgrx code, prefer structured
  errors and controlled PostgreSQL error reporting.

2. SHM slices and page payloads must stay within bounds.

- Any slice from shared memory must be clamped to the advertised layout
  capacity.
- Readers must follow transfer/issuance ownership and must not borrow from pages
  after release.

3. Arrow page layout must match the external schema.

- `slot_encoder`, `page/import`, and `slot_import` must agree on
  `arrow_layout` block shape, type tags, validity layout, and view payload
  ownership.

4. PostgreSQL owns physical table access.

- The active runtime path scans through PostgreSQL `slot_scan`; worker code must
  not reimplement heap visibility, tuple decoding, or TOAST semantics.

5. Lock-free ring buffers require aligned allocation.

- Construct shared-memory rings through `control_transport`, `page/pool`, or
  the approved `lockfree` layout helpers so atomic head/tail words are aligned.
