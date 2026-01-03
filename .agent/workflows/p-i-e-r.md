---
description: P → I → E → R workflow for AI-assisted development
---

# P → I → E → R Workflow

## The Golden Rule
> ⛔ **AI never codes until human says "IMPLEMENT"**

---

## Phases

### P — Plan
**Who**: PRD Agent + Senior Agent + Human
**What**:
1. Human provides requirement
2. PRD Agent documents in `context/prd.md`
3. Senior Agent creates pseudo-code/technical plan
4. Human approves plan

**Gate**: Human must approve before moving to IDENTIFY

---

### I — Identify
**Who**: Senior Agent
**What**:
1. Classify complexity: T1 (Simple) / T2 (Medium) / T3 (Complex)
2. Tag tasks: [S] Shared, [P] Parallel, [D] Dependent
3. Break down into Junior-sized tasks
4. Write to `context/task_queue.md`

**Gate**: Tasks must be assigned before EXECUTE

---

### E — Execute
**Who**: Senior Agent + Junior Agents
**What**:
1. Build [S] Shared tasks first
2. Run [P] Parallel tasks simultaneously
3. Then [D] Dependent tasks sequentially
4. Senior handles complex integration

**Rules**:
- Juniors only see their assigned task
- Juniors cannot make architectural decisions
- Senior integrates all outputs

---

### R — Review
**Who**: Senior Agent + Human
**What**:
1. Senior integrates all components
2. Run tests (if applicable)
3. Validate against PRD
4. Human gives final approval

**R-Gates (Checklist)**:
- [ ] Interfaces match PRD
- [ ] No hidden schema changes
- [ ] Tests exist OR manual test steps written
- [ ] Diff size reasonable
- [ ] Performance noted

---

## Command Vocabulary

Use these explicit commands to control the workflow:

| Command | Effect |
|---------|--------|
| `PLAN` | Start/continue planning phase |
| `IDENTIFY` | Classify and break down tasks |
| `IMPLEMENT` | Begin coding (ONLY after this command) |
| `REVIEW` | Validate and integrate |
| `FIX` | Address issues found in review |

---

## Tier Classification

| Tier | Complexity | Team |
|------|------------|------|
| T1 Simple | 1 API, bug fix, script | You + AI (solo) |
| T2 Medium | 2-3 APIs, ETL, dashboards | Senior + 2 Juniors |
| T3 Complex | Full products, multi-DB | Architect + PRD + Senior + Juniors |

---

## Context File Rules

> Any code change that violates context must update context in the same PR.

| File | Who Writes | Purpose |
|------|------------|---------|
| `context/prd.md` | PRD Agent | Requirements |
| `context/decisions.md` | Senior Agent | Technical decisions |
| `context/task_queue.md` | Senior Agent | Tasks for Juniors |
