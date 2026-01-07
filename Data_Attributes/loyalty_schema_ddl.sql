-- ============================================================================
-- LOYALTY SCHEMA DDL
-- Creates the loyalty data mart tables in Redshift
-- 
-- Run this in Redshift Query Editor before loading data
-- ============================================================================

-- Create the schema
CREATE SCHEMA IF NOT EXISTS loyalty;

-- Grant permissions
GRANT ALL ON SCHEMA loyalty TO admin;
GRANT USAGE ON SCHEMA loyalty TO PUBLIC;

-- ============================================================================
-- DIMENSION TABLES
-- ============================================================================

-- dim_tier: Tier lookup table
DROP TABLE IF EXISTS loyalty.dim_tier;
CREATE TABLE loyalty.dim_tier (
    tier_id VARCHAR(50) NOT NULL,
    tier_name VARCHAR(20) NOT NULL,
    redemption_rate DECIMAL(3,2),
    loaded_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (tier_id)
);

-- Seed default tier data if TierDetailsTable is empty
INSERT INTO loyalty.dim_tier (tier_id, tier_name, redemption_rate) VALUES
('default_gold', 'Gold', 1.00),
('default_silver', 'Silver', 0.70),
('default_bronze', 'Bronze', 0.40);

-- dim_loyalty_users: User dimension with wallet info
DROP TABLE IF EXISTS loyalty.dim_loyalty_users;
CREATE TABLE loyalty.dim_loyalty_users (
    user_id VARCHAR(50) NOT NULL,
    user_name VARCHAR(100),
    phone_number VARCHAR(20),
    phone_normalized VARCHAR(10),       -- For joining with analytics schema
    email VARCHAR(100),
    tier_id VARCHAR(50),
    tier_name VARCHAR(20),
    referral_code VARCHAR(20),
    remaining_coins DECIMAL(12,2) DEFAULT 0,
    total_earned DECIMAL(12,2) DEFAULT 0,
    total_used DECIMAL(12,2) DEFAULT 0,
    signup_date TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (user_id)
)
DISTSTYLE KEY
DISTKEY(phone_normalized)
SORTKEY(user_id);

-- ============================================================================
-- FACT TABLES
-- ============================================================================

-- fact_wallet_transactions: All coin transactions
DROP TABLE IF EXISTS loyalty.fact_wallet_transactions;
CREATE TABLE loyalty.fact_wallet_transactions (
    transaction_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50),
    transaction_type VARCHAR(20),       -- credit/debit
    title VARCHAR(100),                 -- "Signup Bonus", "Referral", etc.
    amount DECIMAL(12,2),
    reason VARCHAR(500),
    status VARCHAR(20),
    created_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (transaction_id)
)
DISTSTYLE KEY
DISTKEY(user_id)
SORTKEY(created_at);

-- fact_referrals: Referral tracking
DROP TABLE IF EXISTS loyalty.fact_referrals;
CREATE TABLE loyalty.fact_referrals (
    referral_id VARCHAR(50) NOT NULL,
    referrer_user_id VARCHAR(50),
    referrer_name VARCHAR(100),
    referred_phone VARCHAR(20),
    referred_phone_normalized VARCHAR(10),
    referred_name VARCHAR(100),
    referred_user_id VARCHAR(50),
    referral_code VARCHAR(20),
    bonus_amount DECIMAL(12,2),
    status VARCHAR(20),                 -- applied/pending
    created_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (referral_id)
)
DISTSTYLE KEY
DISTKEY(referrer_user_id)
SORTKEY(created_at);

-- fact_leads: Lead generation tracking
DROP TABLE IF EXISTS loyalty.fact_leads;
CREATE TABLE loyalty.fact_leads (
    lead_id VARCHAR(50) NOT NULL,
    generator_user_id VARCHAR(50),
    generator_name VARCHAR(100),
    lead_name VARCHAR(100),
    lead_phone VARCHAR(20),
    occasion_name VARCHAR(100),
    lead_stage VARCHAR(100),
    estimated_value DECIMAL(12,2),
    created_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (lead_id)
)
DISTSTYLE KEY
DISTKEY(generator_user_id)
SORTKEY(created_at);

-- fact_withdrawals: Withdrawal requests
DROP TABLE IF EXISTS loyalty.fact_withdrawals;
CREATE TABLE loyalty.fact_withdrawals (
    withdrawal_id VARCHAR(50) NOT NULL,
    user_id VARCHAR(50),
    user_name VARCHAR(100),
    requested_amount DECIMAL(12,2),
    approved_amount DECIMAL(12,2),
    status VARCHAR(20),                 -- Pending/Approved/Rejected
    bank_id VARCHAR(50),
    upi_id VARCHAR(100),
    created_at TIMESTAMP,
    processed_at TIMESTAMP,
    loaded_at TIMESTAMP DEFAULT GETDATE(),
    PRIMARY KEY (withdrawal_id)
)
DISTSTYLE KEY
DISTKEY(user_id)
SORTKEY(created_at);

-- ============================================================================
-- VIEWS (Optional - for common queries)
-- ============================================================================

-- View: Active users with coin balance
CREATE OR REPLACE VIEW loyalty.v_active_users AS
SELECT 
    user_id,
    user_name,
    phone_normalized,
    tier_name,
    remaining_coins,
    remaining_coins * CASE tier_name 
        WHEN 'Gold' THEN 1.0 
        WHEN 'Silver' THEN 0.7 
        ELSE 0.4 
    END as rupee_value
FROM loyalty.dim_loyalty_users
WHERE remaining_coins > 0;

-- View: Top referrers
CREATE OR REPLACE VIEW loyalty.v_top_referrers AS
SELECT 
    referrer_user_id,
    referrer_name,
    COUNT(*) as referral_count,
    COUNT(CASE WHEN referred_user_id IS NOT NULL THEN 1 END) as converted_count
FROM loyalty.fact_referrals
GROUP BY referrer_user_id, referrer_name
ORDER BY referral_count DESC;

-- View: Daily coin activity
CREATE OR REPLACE VIEW loyalty.v_daily_coin_activity AS
SELECT 
    DATE(created_at) as activity_date,
    SUM(CASE WHEN transaction_type = 'credit' THEN amount ELSE 0 END) as credits,
    SUM(CASE WHEN transaction_type = 'debit' THEN ABS(amount) ELSE 0 END) as debits
FROM loyalty.fact_wallet_transactions
GROUP BY DATE(created_at)
ORDER BY activity_date DESC;

-- ============================================================================
-- GRANT PERMISSIONS
-- ============================================================================

-- Note: In Redshift, views are included in "TABLES" grant
GRANT SELECT ON ALL TABLES IN SCHEMA loyalty TO PUBLIC;

-- ============================================================================
-- VERIFICATION QUERIES
-- ============================================================================

-- Run these after loading data to verify:
/*
SELECT 'dim_tier' as table_name, COUNT(*) as row_count FROM loyalty.dim_tier
UNION ALL SELECT 'dim_loyalty_users', COUNT(*) FROM loyalty.dim_loyalty_users
UNION ALL SELECT 'fact_wallet_transactions', COUNT(*) FROM loyalty.fact_wallet_transactions
UNION ALL SELECT 'fact_referrals', COUNT(*) FROM loyalty.fact_referrals
UNION ALL SELECT 'fact_leads', COUNT(*) FROM loyalty.fact_leads
UNION ALL SELECT 'fact_withdrawals', COUNT(*) FROM loyalty.fact_withdrawals;
*/
