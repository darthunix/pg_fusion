---
title: Memory Bank Index
updated_at: "2025-11-29"
---

# Memory Bank Index

## Reading Order

1. CODEX memory model: `CODEX_MEMORY_MODEL.md`
2. Architecture overview: `architecture.md`
3. Component notes under `components/`
4. Decisions (ADR-lite) under `decisions/`
5. Invariants summary: `invariants.md`

## Conventions

- Every memo is Markdown with YAML frontmatter (`id`, `type`, `scope`, `tags`, `updated_at`, `importance`).
- Allowed types: `invariant | decision | fact | gotcha | todo | note`.
- Agents must load all invariants with `importance >= 0.8` before planning changes.

## Directory Layout

```
/ai/memory/
  architecture.md         # repo-wide architecture
  invariants.md           # curated list of key invariants
  components/             # per-component memos
  decisions/              # ADR-lite decisions
  CODEX_MEMORY_MODEL.md   # the reference model
```
