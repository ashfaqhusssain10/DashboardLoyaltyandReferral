"""
Services package initialization
"""
from .dynamodb_service import db_service
from .user_service import get_user_by_id, search_users, get_all_users
from .wallet_service import (
    get_wallet_by_user, 
    get_transactions_by_user,
    get_total_coins_in_system,
    get_active_users_count,
    get_all_wallets
)
from .lead_service import get_leads_by_user, get_today_leads_count, get_today_leads
from .referral_service import (
    get_referrals_by_user, 
    get_today_referrals_count,
    get_weekly_referral_stats,
    get_referral_stats_by_range,
    get_today_referrals
)
from .withdrawal_service import (
    get_all_withdrawals,
    get_pending_count,
    get_total_pending_amount,
    get_pending_withdrawals,
    get_withdrawals_by_user,
    approve_withdrawal,
    reject_withdrawal
)
from .tier_service import get_tier_name, get_all_tiers
from .order_service import (
    get_orders_by_user,
    get_order_revenue_by_user,
    get_referral_revenue_for_user
)
