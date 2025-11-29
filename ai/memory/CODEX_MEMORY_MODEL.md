Below is a clean, compact memory dump for Codex/LLM agents that captures the key ideas of a “human‑readable memory bank” and “when to start a vector index.” Use as a system prompt, agent README, or memory-bank insert.

---

# 📦 Codex Memory Model — Dump

## 1. Big Picture

Project memory should be human‑readable, durable, and agent‑friendly.
Primary format: Markdown + YAML frontmatter.
Top directory:

```
/ai/
  memory/      # invariants, ADR/decisions, architecture
  log/         # project diary
  README.md    # how to use this
```

---

## 2. Memory Record Format (Knowledge Fragments)

Every memory fragment is Markdown with a YAML frontmatter.

```markdown
---
id: inv-raft-001
type: invariant        # invariant | decision | fact | gotcha | todo | note
scope: planner
tags: ["raft", "sharding"]
updated_at: "2025-11-29"
importance: 0.95       # 0..1 — how critical it is to follow
---

# Invariant: The Planner Operates Within a Single Raft Group

(human‑readable explanation)
```

Why this format:

- readable as documentation,
- easy to parse by agents,
- YAML provides the structure,
- Markdown provides narrative and context.

---

## 3. Memory Types

Use a small fixed set:

- `invariant` — a principle that must not be violated
- `decision` — an architectural decision (ADR‑lite)
- `fact` — important information about the system
- `gotcha` — pitfalls and caveats
- `todo` — long‑lived improvements, not tasks
- `note` — useful observations

This helps agents:
for architecture — look at `invariant` and `decision`,
for debugging — `gotcha`,
for system analysis — `fact`.

---

## 4. Separate Memory Files

Example layout:

```
/ai/memory/index.md
/ai/memory/invariants.md
/ai/memory/architecture.md
/ai/memory/components/planner.md
/ai/memory/components/storage.md
/ai/memory/decisions/0001-sharding-model.md
/ai/memory/decisions/0002-datafusion-integration.md
```

`index.md` describes structure and reading order.

---

## 5. Instructions for LLM Agents

Agents must:

1. Read `/ai/memory/index.md` first.
2. Before architecture answers, load:
   - `architecture.md`
   - relevant components
   - all `invariant` with `importance >= 0.8`
3. Before code generation, honor all invariants in `/ai/memory`.
4. If an invariant would be violated, name it and propose an alternative.
5. Avoid stale notes (`updated_at` too old or a `deprecated` flag, if present).

---

## 6. When to Start a Vector Index

Two layers of indexing:

### Layer A — Vector Index of Memory (`/ai/memory`)

Start early, after ~10–20 meaningful notes. Cheap, stable, useful.

### Layer B — Vector Index of Source Code

Start when 2–3 conditions hold:

- multiple subsystems/crates,
- architecture relatively stabilized,
- “where is X implemented?” requires significant search,
- context does not fit a single prompt.

Initial scope only:

1. public APIs,
2. key modules (planner/storage/executor),
3. doc comments and gotchas.

Grow beyond this as the project scales.

---

## 7. Why Not Start Too Early

- code structure changes rapidly → index rots quickly;
- noise outweighs signal;
- maintenance cost > value early on;
- while the project is small, LSP/grep is enough.

---

## 8. Practical Timeline

### Phase 1: first weeks

Create `/ai/memory/*.md` with no index yet.

### Phase 2: 10–20 memory fragments exist

Start the memory vector index (still not code).

### Phase 3: architecture stabilized, project grew

Start the source code index.

---

## 9. Project Log (Optional)

In `/ai/log/YYYY-MM-DD.md` keep human‑readable diaries:

- key decisions,
- observations,
- issues.

This is a RAG data source, but not part of invariants.

---

# ✔ Recommended Standard of Memory for Codex/LLM Agents

Optionally, we can:

- generate templates (memory template generator),
- provide JSON Schema for frontmatter validation,
- generate an example `/ai/memory` for your project (pg_fusion / picodata),
- suggest an index refresh strategy (pre‑commit hook + partial refresh).
