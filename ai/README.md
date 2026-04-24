# Agent Context

This directory is the living context for agents and humans working on
`pg_fusion`. It is intentionally small and current; historical notes belong in
git history, not here.

## Structure

```
/ai/
  README.md         # this file
  architecture.md   # current repo architecture
  invariants.md     # rules that should not be violated
  gotchas.md        # practical pitfalls
  components/       # short component notes
```

## Reading Order

1. Read `architecture.md` for the active runtime shape.
2. Read `invariants.md` before planning code changes.
3. Load relevant files from `components/` for the subsystem being changed.
4. Check `gotchas.md` when touching pgrx, shared memory, scan execution, or
   page-backed Arrow data.

## Maintenance

- After behavior or architecture changes, update the relevant file here in the
  same change.
- Keep entries short and active. Remove stale history instead of preserving it
  as agent context.
- If a new architectural decision needs a durable explanation, summarize the
  current consequence in `architecture.md`, `invariants.md`, or a component
  note. Use git history for the full archaeological trail.
