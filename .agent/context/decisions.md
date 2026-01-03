# Decisions Log

> This file is maintained by the **Senior Agent**. Document all technical decisions with rationale.

---

## Format

```
### DEC-XXX: [Decision Title]
**Date**: YYYY-MM-DD
**Status**: Proposed | Approved | Superseded
**Context**: Why this decision was needed
**Decision**: What was decided
**Rationale**: Why this option was chosen
**Alternatives Considered**: Other options that were rejected
```

---

## Decisions

### DEC-001: Use Streamlit for Admin Dashboard
**Date**: 2024-12-31
**Status**: Approved
**Context**: Need a quick-to-build admin dashboard for internal use
**Decision**: Use Streamlit instead of React/Next.js
**Rationale**: 
- Python-native (matches backend)
- Rapid prototyping
- Built-in components for data visualization
- Low learning curve
**Alternatives Considered**:
- React + Next.js (rejected: overkill for internal admin tool)
- Flask + Jinja (rejected: more boilerplate needed)

---

### DEC-002: AWS DynamoDB as Data Source
**Date**: 2024-12-31
**Status**: Approved
**Context**: Data already exists in DynamoDB tables
**Decision**: Connect directly to existing DynamoDB tables
**Rationale**: No migration needed, data already structured
**Alternatives Considered**: None (existing infrastructure)

---

<!-- Add new decisions below -->

### DEC-003: Pre-computed Aggregates for Dashboard Performance
**Date**: 2026-01-03
**Status**: Approved
**Context**: Dashboard KPIs and leaderboards were causing 10+ second load times due to full table scans on WalletTable (184k+ items) and other large tables.
**Decision**: Implement DynamoDB Streams + Lambda + AdminAggregatesTable architecture
**Rationale**: 
- Pre-computed metrics reduce dashboard latency from 10+ seconds to <100ms
- DynamoDB Streams provide real-time updates without polling
- New AdminAggregatesTable isolates aggregates from production data (zero-risk to existing tables)
- Fallback mechanism allows graceful degradation if aggregates unavailable
**Alternatives Considered**:
- Application-level caching (rejected: still requires initial full scan)
- Redis/ElastiCache (rejected: additional infrastructure complexity)
- Adding STATS item to existing tables (rejected: modifies production tables)

**Files Created**:
- `app/services/aggregates_service.py` - Service to read aggregates
- `scripts/seed_aggregates.py` - Initial data population
- `lambda/aggregates_updater/handler.py` - Stream processor

---
