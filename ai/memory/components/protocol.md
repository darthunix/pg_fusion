---
id: comp-protocol-0001
type: fact
scope: protocol
tags: ["ipc", "control", "data", "tuple", "bitmap"]
updated_at: "2026-01-07"
importance: 0.7
---

# Component: Protocol

- Control: `Parse/Metadata/Bind/Optimize/Translate/Explain/BeginScan/ExecScan/EndScan/ExecReady/ColumnLayout`.
- Data: `Heap` (requests + metadata for SHM page + vis bitmap `vis_len`), `Eof` per‑scan (u64 scan_id).
- Column layout: `PgAttrWire { atttypid, typmod, attlen, attalign, attbyval, nullable }` to drive result encoding.
- Tuples: `encode_wire_tuple` + `decode_wire_tuple` with header, null bitmap, and aligned data area.
