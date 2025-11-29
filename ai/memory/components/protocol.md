---
id: comp-protocol-0001
type: fact
scope: protocol
tags: ["wire", "tuple", "ipc", "msgpack"]
updated_at: "2025-11-29"
importance: 0.85
---

# Component: Protocol

## Control Packets

- Parse/Metadata/Bind/Optimize/Translate/BeginScan/ExecScan/EndScan/Explain/Failure/ExecReady.

## Data Packets

- Heap: meta with (scan_id, slot_id, table_oid, blkno, num_offsets, vis_len); page/bitmap bytes reside in SHM.
- Results: `result::write_frame(u32_len + payload)`; payload is a “WireMinimalTuple”:
  - Header (nattrs, flags, hoff, bitmap_bytes, data_bytes), null bitmap (LSB‑first), aligned attributes.

## Supported Types

- boolean, int2/4/8, float4/8, utf8, date, time64(us), timestamp(us), interval month‑day‑nano.
- Alignment and attlen must match `PgAttrWire`.
