# plan_codec

`plan_codec` serializes backend-built DataFusion logical plans into a versioned
wire payload and restores them on the worker side.

The crate is intentionally narrow:

- it round-trips the logical plan shapes currently emitted by `pg/plan_builder`
- the outer envelope is MsgPack, while built-in DataFusion nodes and
  expressions stay delegated to `datafusion-proto`
- `scan_node::PgScanNode` carries only `scan_id` inside the protobuf logical
  plan; the full `PgScanSpec` table lives alongside it in the outer envelope
- the payload carries `scan_id`, but never carries snapshot identity
- callers provide the output/input buffers directly via `bytes::BufMut` /
  `bytes::Buf`; `plan_codec` does not require a single staged `Vec<u8>`
- expr-level subqueries are out of scope; `pg/plan_builder` rejects them before
  serialization, so this codec only needs to support the leaf-scan planning
  subset

It does not own page rollover or transport. Later `page/plan_flow` is
responsible for chunking and carrier interaction.

## Example

```rust,ignore
use bytes::BytesMut;
use plan_builder::{PlanBuildInput, PlanBuilder};
use plan_codec::{decode_plan_from, encode_plan_into};

let built = PlanBuilder::new().build(PlanBuildInput {
    sql: "SELECT id, payload FROM public.orders WHERE id > 10 LIMIT 32",
    params: vec![],
})?;

let mut sink = BytesMut::new();
encode_plan_into(&built.logical_plan, &mut sink)?;

let mut source = sink.freeze();
let decoded = decode_plan_from(&mut source)?;

assert_eq!(
    built.logical_plan.display_indent().to_string(),
    decoded.display_indent().to_string(),
);
```
