---
id: dec-0003
type: decision
scope: protocol
tags: ["wire", "tuple", "minimaltuple", "alignment"]
updated_at: "2025-11-29"
importance: 0.85
---

# Decision: WireMinimalTuple With Explicit Alignment

## Motivation

Backend needs a fast, correct path to `MinimalTuple` without complex conversions.

## Details

- Header (nattrs, flags, hoff, bitmap_bytes, data_bytes).
- Null bitmap (LSB‑first), then attributes aligned per `attalign`.
- varlena transmitted as `u32` LE length + bytes.
