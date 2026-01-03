# Task Queue

> **Managed by**: Senior Agent  
> **Current Sprint**: Gap Implementation G01-G05

---

## 🔴 ACTIVE TASKS

### TASK-001: Backend Leaderboard Services
**Assigned to**: Junior Backend Agent  
**Priority**: High  
**Status**: 🔄 In Progress

**Context**:
Add 5 new service functions for leaderboard data.

**Deliverables**:
1. `get_top_coin_holders(limit=5)` → WalletTable, sort by remainingAmount DESC
2. `get_top_referrers(limit=5)` → TierReferralTable, count by userId
3. `get_top_lead_generators(limit=5)` → LeadTable, count by userId  
4. `get_top_earners(limit=5)` → WalletTransactionTable, sum credits by userId
5. `get_daily_coin_activity(days=7)` → WalletTransactionTable, group by date

**Files to modify**:
- `app/services/wallet_service.py`
- `app/services/referral_service.py`
- `app/services/lead_service.py`

---

### TASK-002: Frontend Dashboard Leaderboards
**Assigned to**: Junior Frontend Agent  
**Priority**: High  
**Status**: ⏳ Waiting for TASK-001
**Depends on**: TASK-001

**Context**:
Add leaderboard section and daily coin chart to Dashboard page.

**Deliverables**:
1. 4 leaderboard cards (Top Coins, Top Referrers, Top Leads, Top Earners)
2. Daily coin activity chart (credits vs debits)

**Files to modify**:
- `pages/1_Dashboard.py`

---

## ✅ COMPLETED TASKS
*(Previous sprint tasks)*
