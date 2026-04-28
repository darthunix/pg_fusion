# plan_codec

`plan_codec` serializes backend-built DataFusion logical plans into a versioned
wire payload and restores them on the worker side.

The crate is intentionally narrow:

- it round-trips the logical plan shapes currently emitted by `pg/plan_builder`
- the outer envelope is MsgPack, while built-in DataFusion nodes and
  expressions stay delegated to `datafusion-proto`
- `scan_node::PgScanNode` carries only `scan_id` inside the protobuf logical
  plan; the full `PgScanSpec` table lives alongside it in the outer envelope
- `scan_node::PgCteRefNode` carries its query-local CTE id, output schema, and
  per-reference projection/fetch metadata; its child plan is the CTE producer
- the payload carries `scan_id`, but never carries snapshot identity
- callers use streaming sessions and provide page-sized chunks directly;
  `plan_codec` does not require a single staged `Vec<u8>` for the full plan
- expr-level subqueries are out of scope; `pg/plan_builder` rejects them before
  serialization, so this codec only needs to support the leaf-scan planning
  subset

It does not own page rollover or transport. Later `page/plan_flow` is
responsible for page orchestration and carrier interaction.

## Example

```rust,ignore
use plan_builder::{PlanBuildInput, PlanBuilder};
use plan_codec::{DecodeProgress, EncodeProgress, PlanDecodeSession, PlanEncodeSession};

let built = PlanBuilder::new().build(PlanBuildInput {
    sql: "SELECT id, payload FROM public.orders WHERE id > 10 LIMIT 32",
    params: vec![],
})?;

let mut encoder = PlanEncodeSession::new(&built.logical_plan)?;
let mut encoded = Vec::new();

loop {
    let mut page = [0u8; 4096];
    match encoder.write_chunk(&mut page)? {
        EncodeProgress::NeedMoreOutput { written } => {
            encoded.extend_from_slice(&page[..written]);
        }
        EncodeProgress::Done { written } => {
            encoded.extend_from_slice(&page[..written]);
            break;
        }
    }
}

let mut decoder = PlanDecodeSession::new();
for chunk in encoded.chunks(4096) {
    let progress = decoder.push_chunk(chunk)?;
    assert!(matches!(progress, DecodeProgress::NeedMoreInput));
}

let decoded = match decoder.finish_input()? {
    DecodeProgress::Done(plan) => *plan,
    DecodeProgress::NeedMoreInput => unreachable!("EOF must finish or fail"),
};

assert_eq!(
    built.logical_plan.display_indent().to_string(),
    decoded.display_indent().to_string(),
);
```
