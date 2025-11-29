# Codex Memory Model for This Repo

This repository adopts a human-readable memory model for agents and humans.

Structure:

```
/ai/
  memory/      # invariants, decisions, architecture, components
  log/         # project log (optional, day-by-day)
  README.md    # this file — how to use the memory bank
```

Agent usage quickstart:

- Read `/ai/memory/index.md` first.
- For architecture answers: load `architecture.md`, relevant component memos, and all invariants with `importance >= 0.8`.
- For code generation: respect all invariants in `/ai/memory`.
- If an invariant would be violated, state which one and propose an alternative.

See `/ai/memory/CODEX_MEMORY_MODEL.md` for the complete model description.

Agent workflow requirement:

- After implementing or changing behavior, update the corresponding files under `/ai/memory` (components, decisions, invariants, architecture). Keep the memory bank current so future agents have accurate context.
