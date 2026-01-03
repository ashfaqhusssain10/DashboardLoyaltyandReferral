# Product Requirements Document (PRD)

> **Maintained by**: PRD Agent  
> **Last Updated**: 2026-01-02  
> **Status**: 🔄 In Progress - Gaps Identified

---

## Project: Admin Control Tower V1

### Overview
Admin dashboard for managing the CraftMyPlate loyalty and referral system with coin-based rewards.

### Tech Stack
- **Frontend**: Streamlit (Python)
- **Database**: AWS DynamoDB

---

## Requirements Tracking

### ✅ IMPLEMENTED (10)

| ID | Requirement | Page |
|----|-------------|------|
| R01 | Loyalty and Referral Dashboard | Dashboard |
| R02 | Request of Withdrawals (Accept/Reject) | Withdrawals |
| R03 | Daily basis coin transactions view | Coin Transactions |
| R04 | Search by Name/Phone Number | Coin Transactions |
| R05 | Show Tier/Balance/Coin History | Coin Transactions |
| R06 | Referral History per user | Coin Transactions |
| R07 | Total Coins in Market (KPI) | Dashboard |
| R08 | Realizable Market with tier rates | Coin Transactions |
| R09 | User joining date (created_at) | Coin Transactions |
| R10 | Submitted Lead history | Coin Transactions |

---

### ⚠️ GAPS TO ADDRESS (6)

| ID | Requirement | Priority | Proposed Solution |
|----|-------------|----------|-------------------|
| G01 | Highest Coin Balance users | High | Dashboard Leaderboard - Top 5 users by coins |
| G02 | Highest Person People Referred (Top Referrers) | High | Dashboard Leaderboard - Top 5 by referral count |
| G03 | Similarly for Lead (Top Lead Generators) | High | Dashboard Leaderboard - Top 5 by lead count |
| G04 | Highest Earner | Medium | Dashboard Leaderboard - Top 5 by total coins earned |
| G05 | Coin Usage Daily Basis (Trend) | Medium | Dashboard - Daily coin credit/debit chart |
| G06 | Referral Revenue Calculation | Low | User Profile - Show revenue from their referrals |

---

### ❌ OUT OF SCOPE (Next Module)

| Requirement | Module |
|-------------|--------|
| Modify platter level variables | Menu Management |
| Prices values for each item | Menu Management |
| Factor level variables | Pricing Config |

---

## Gap Implementation Details

### G01: Highest Coin Balance Users
**Data Source**: WalletTable  
**Logic**: Sort by `remainingAmount` DESC, take top 5  
**Display**: Leaderboard card on Dashboard

### G02: Top Referrers
**Data Source**: TierReferralTable  
**Logic**: Group by `userId`, count referrals, sort DESC, take top 5  
**Display**: Leaderboard card on Dashboard

### G03: Top Lead Generators
**Data Source**: LeadTable  
**Logic**: Group by `userId`, count leads, sort DESC, take top 5  
**Display**: Leaderboard card on Dashboard

### G04: Highest Earner
**Data Source**: WalletTransactionTable  
**Logic**: Group by `userId`, sum credits where title='Credit', sort DESC, take top 5  
**Display**: Leaderboard card on Dashboard

### G05: Daily Coin Usage Trend
**Data Source**: WalletTransactionTable  
**Logic**: Group by date, sum credits and debits separately for 7 days  
**Display**: Line/Bar chart on Dashboard

### G06: Referral Revenue Calculation
**Data Source**: TierReferralTable + OrderTable  
**Logic**: For each referral, check if they placed orders, sum order values  
**Display**: In User Profile under Referral History tab

---

## Verification Checklist

- [x] Dashboard with KPIs
- [x] Coin Transactions with search
- [x] Withdrawal Accept/Reject with confirmation
- [x] User profile with history tabs
- [x] Tier-specific rates (Gold×1.00, Silver×0.70, Bronze×0.40)
- [ ] **Leaderboards on Dashboard**
- [ ] **Daily coin trend chart**
- [ ] **Referral revenue calculation**
