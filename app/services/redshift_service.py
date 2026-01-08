"""
Redshift Service - Query layer for Redshift loyalty data mart

This service provides optimized analytics queries against the Redshift
loyalty schema. Use this for:
- Tier-wise coin statistics
- Leaderboards (top holders, earners, referrers)
- Historical charts and trends

For real-time data (today's KPIs, user search), continue using DynamoDB.
"""

import os
import psycopg2
from psycopg2.extras import RealDictCursor
from typing import List, Dict, Optional, Any
from datetime import date, datetime, timedelta
from contextlib import contextmanager
from functools import lru_cache
import logging

# Load environment variables from .env file
from dotenv import load_dotenv
load_dotenv()

logger = logging.getLogger(__name__)

# Redshift connection config from environment
REDSHIFT_CONFIG = {
    'host': os.environ.get('REDSHIFT_HOST', 'data-pipeline-cluster.cnvwvj5onh5a.ap-south-1.redshift.amazonaws.com'),
    'port': int(os.environ.get('REDSHIFT_PORT', 5439)),
    'database': os.environ.get('REDSHIFT_DATABASE', 'datawarehouse'),
    'user': os.environ.get('REDSHIFT_USER', 'admin'),
    'password': os.environ.get('REDSHIFT_PASSWORD', '')
}

# Tier conversion rates (same as wallet_service)
TIER_RATES = {
    'Bronze': 0.40,
    'Silver': 0.70,
    'Gold': 1.00,
    'Unknown': 0.40
}


class RedshiftService:
    """Service for querying Redshift loyalty data mart."""
    
    def __init__(self):
        self._connection = None
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections."""
        conn = None
        try:
            conn = psycopg2.connect(**REDSHIFT_CONFIG)
            yield conn
        except Exception as e:
            logger.error(f"Redshift connection error: {e}")
            raise
        finally:
            if conn:
                conn.close()
    
    def execute_query(self, query: str, params: tuple = None) -> List[Dict]:
        """Execute a query and return results as list of dicts."""
        try:
            with self.get_connection() as conn:
                with conn.cursor(cursor_factory=RealDictCursor) as cur:
                    cur.execute(query, params)
                    results = cur.fetchall()
                    return [dict(row) for row in results]
        except Exception as e:
            logger.error(f"Query error: {e}")
            return []
    
    # =========================================================================
    # TIER-WISE STATISTICS
    # =========================================================================
    
    def get_coins_by_tier(self) -> Dict[str, Dict]:
        """
        Get total coins grouped by tier with rupee value.
        Replaces: wallet_service.get_coins_by_tier()
        
        Returns: {tier_name: {'coins': X, 'rupees': Y, 'users': Z, 'rate': R}}
        """
        query = """
        SELECT 
            tier_name,
            COUNT(*) as user_count,
            COALESCE(SUM(remaining_coins), 0) as total_coins
        FROM loyalty.dim_loyalty_users
        GROUP BY tier_name
        """
        
        results = self.execute_query(query)
        
        tier_data = {}
        for row in results:
            tier = row['tier_name'] or 'Unknown'
            coins = float(row['total_coins'] or 0)
            rate = TIER_RATES.get(tier, 0.40)
            tier_data[tier] = {
                'coins': coins,
                'rupees': coins * rate,
                'users': int(row['user_count']),
                'rate': rate
            }
        
        # Ensure all tiers exist
        for tier in ['Gold', 'Silver', 'Bronze', 'Unknown']:
            if tier not in tier_data:
                tier_data[tier] = {'coins': 0, 'rupees': 0, 'users': 0, 'rate': TIER_RATES[tier]}
        
        return tier_data
    
    def get_total_coins_in_system(self) -> float:
        """Get total coins across all wallets."""
        query = "SELECT COALESCE(SUM(remaining_coins), 0) as total FROM loyalty.dim_loyalty_users"
        results = self.execute_query(query)
        return float(results[0]['total']) if results else 0.0
    
    def get_active_users_count(self) -> int:
        """Count users with coin balance > 0."""
        query = "SELECT COUNT(*) as count FROM loyalty.dim_loyalty_users WHERE remaining_coins > 0"
        results = self.execute_query(query)
        return int(results[0]['count']) if results else 0
    
    # =========================================================================
    # LEADERBOARDS
    # =========================================================================
    
    def get_top_coin_holders(self, limit: int = 10) -> List[Dict]:
        """
        Get users with highest coin balance.
        Replaces: wallet_service.get_top_coin_holders()
        """
        query = """
        SELECT 
            user_id,
            user_name,
            tier_name,
            remaining_coins as coins,
            phone_number
        FROM loyalty.dim_loyalty_users
        WHERE remaining_coins > 0
        ORDER BY remaining_coins DESC
        LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_top_earners(self, limit: int = 10) -> List[Dict]:
        """
        Get users who earned the most coins (total credits).
        Replaces: wallet_service.get_top_earners()
        """
        query = """
        SELECT 
            u.user_id,
            u.user_name,
            u.tier_name,
            COALESCE(SUM(t.amount), 0) as total_earned
        FROM loyalty.dim_loyalty_users u
        LEFT JOIN loyalty.fact_wallet_transactions t ON u.user_id = t.user_id
        WHERE t.transaction_type = 'credit'
          AND t.title IN ('Signup Bonus', 'Referral', 'Lead Bonus', 'Added to Wallet')
        GROUP BY u.user_id, u.user_name, u.tier_name
        ORDER BY total_earned DESC
        LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_top_referrers(self, limit: int = 10) -> List[Dict]:
        """
        Get users with most referrals.
        Replaces: referral_service.get_top_referrers()
        """
        query = """
        SELECT 
            referrer_user_id as user_id,
            referrer_name as user_name,
            COUNT(*) as referral_count
        FROM loyalty.fact_referrals
        WHERE referrer_user_id IS NOT NULL AND referrer_user_id != ''
        GROUP BY referrer_user_id, referrer_name
        ORDER BY referral_count DESC
        LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_top_lead_generators(self, limit: int = 10) -> List[Dict]:
        """
        Get users who generated the most leads.
        Replaces: lead_service.get_top_lead_generators()
        """
        query = """
        SELECT 
            generator_user_id as user_id,
            generator_name as user_name,
            COUNT(*) as lead_count
        FROM loyalty.fact_leads
        WHERE generator_user_id IS NOT NULL AND generator_user_id != ''
        GROUP BY generator_user_id, generator_name
        ORDER BY lead_count DESC
        LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_top_withdrawers(self, limit: int = 10) -> List[Dict]:
        """
        Get users with most withdrawal requests.
        """
        query = """
        SELECT 
            user_id,
            user_name,
            COUNT(*) as withdrawal_count,
            COALESCE(SUM(requested_amount), 0) as total_requested
        FROM loyalty.fact_withdrawals
        WHERE user_id IS NOT NULL AND user_id != ''
        GROUP BY user_id, user_name
        ORDER BY withdrawal_count DESC
        LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_top_added_to_wallet(self, limit: int = 10) -> List[Dict]:
        """
        Get users with most 'Added to Wallet' credits (referral bonuses).
        Replaces: wallet_service.get_top_added_to_wallet()
        """
        query = """
        SELECT 
            u.user_id,
            u.user_name,
            COALESCE(SUM(t.amount), 0) as total_added
        FROM loyalty.dim_loyalty_users u
        LEFT JOIN loyalty.fact_wallet_transactions t ON u.user_id = t.user_id
        WHERE t.title = 'Added to Wallet' AND t.amount > 0
        GROUP BY u.user_id, u.user_name
        HAVING SUM(t.amount) > 0
        ORDER BY total_added DESC
        LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_referral_stats_by_range(self, start_date: date, end_date: date) -> List[Dict]:
        """
        Get daily referral counts for a date range (for chart).
        Replaces: referral_service.get_referral_stats_by_range()
        """
        query = """
        SELECT 
            DATE(created_at) as date,
            COUNT(*) as count
        FROM loyalty.fact_referrals
        WHERE DATE(created_at) BETWEEN %s AND %s
        GROUP BY DATE(created_at)
        ORDER BY date ASC
        """
        return self.execute_query(query, (start_date, end_date))
    
    # =========================================================================
    # DAILY ACTIVITY CHARTS
    # =========================================================================
    
    def get_daily_coin_activity(self, days: int = 30) -> List[Dict]:
        """
        Get daily coin credits and debits for charting.
        Replaces: wallet_service.get_daily_coin_activity()
        """
        query = """
        SELECT 
            DATE(created_at) as date,
            SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as credits,
            SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as debits
        FROM loyalty.fact_wallet_transactions
        WHERE created_at >= DATEADD(day, -%s, CURRENT_DATE)
        GROUP BY DATE(created_at)
        ORDER BY date ASC
        """
        return self.execute_query(query, (days,))
    
    def get_daily_coin_activity_by_range(self, start_date: date, end_date: date) -> List[Dict]:
        """
        Get daily coin activity for a custom date range.
        """
        query = """
        SELECT 
            DATE(created_at) as date,
            SUM(CASE WHEN amount > 0 THEN amount ELSE 0 END) as credits,
            SUM(CASE WHEN amount < 0 THEN ABS(amount) ELSE 0 END) as debits
        FROM loyalty.fact_wallet_transactions
        WHERE DATE(created_at) BETWEEN %s AND %s
        GROUP BY DATE(created_at)
        ORDER BY date ASC
        """
        return self.execute_query(query, (start_date, end_date))
    
    def get_daily_referral_activity(self, days: int = 30) -> List[Dict]:
        """Get daily referral counts for charting."""
        query = """
        SELECT 
            DATE(created_at) as date,
            COUNT(*) as referral_count
        FROM loyalty.fact_referrals
        WHERE created_at >= DATEADD(day, -%s, CURRENT_DATE)
        GROUP BY DATE(created_at)
        ORDER BY date ASC
        """
        return self.execute_query(query, (days,))
    
    def get_daily_lead_activity(self, days: int = 30) -> List[Dict]:
        """Get daily lead counts for charting."""
        query = """
        SELECT 
            DATE(created_at) as date,
            COUNT(*) as lead_count
        FROM loyalty.fact_leads
        WHERE created_at >= DATEADD(day, -%s, CURRENT_DATE)
        GROUP BY DATE(created_at)
        ORDER BY date ASC
        """
        return self.execute_query(query, (days,))
    
    # =========================================================================
    # TIME-FILTERED LEADERBOARDS
    # =========================================================================
    
    def get_top_referrers_by_period(self, limit: int = 10, period: str = 'all') -> List[Dict]:
        """
        Get users with most referrals for a time period.
        period: 'all' | 'week' | 'today'
        """
        date_filter = ""
        if period == 'week':
            date_filter = "AND created_at >= DATEADD(day, -7, CURRENT_DATE)"
        elif period == 'today':
            date_filter = "AND DATE(created_at) = CURRENT_DATE"
        
        query = f"""
        SELECT 
            referrer_user_id as user_id,
            referrer_name as user_name,
            COUNT(*) as referral_count
        FROM loyalty.fact_referrals
        WHERE referrer_user_id IS NOT NULL AND referrer_user_id != ''
        {date_filter}
        GROUP BY referrer_user_id, referrer_name
        ORDER BY referral_count DESC
        LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_top_lead_generators_by_period(self, limit: int = 10, period: str = 'all') -> List[Dict]:
        """
        Get users who generated the most leads for a time period.
        period: 'all' | 'week' | 'today'
        """
        date_filter = ""
        if period == 'week':
            date_filter = "AND created_at >= DATEADD(day, -7, CURRENT_DATE)"
        elif period == 'today':
            date_filter = "AND DATE(created_at) = CURRENT_DATE"
        
        query = f"""
        SELECT 
            generator_user_id as user_id,
            generator_name as user_name,
            COUNT(*) as lead_count
        FROM loyalty.fact_leads
        WHERE generator_user_id IS NOT NULL AND generator_user_id != ''
        {date_filter}
        GROUP BY generator_user_id, generator_name
        ORDER BY lead_count DESC
        LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_top_earners_by_period(self, limit: int = 10, period: str = 'all') -> List[Dict]:
        """
        Get users who earned the most coins for a time period.
        period: 'all' | 'week' | 'today'
        """
        date_filter = ""
        if period == 'week':
            date_filter = "AND t.created_at >= DATEADD(day, -7, CURRENT_DATE)"
        elif period == 'today':
            date_filter = "AND DATE(t.created_at) = CURRENT_DATE"
        
        query = f"""
        SELECT 
            u.user_id,
            u.user_name,
            u.tier_name,
            COALESCE(SUM(t.amount), 0) as total_earned
        FROM loyalty.dim_loyalty_users u
        LEFT JOIN loyalty.fact_wallet_transactions t ON u.user_id = t.user_id
        WHERE t.transaction_type = 'credit'
          AND t.title IN ('Signup Bonus', 'Referral', 'Lead Bonus', 'Added to Wallet')
          {date_filter}
        GROUP BY u.user_id, u.user_name, u.tier_name
        ORDER BY total_earned DESC
        LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_top_added_to_wallet_by_period(self, limit: int = 10, period: str = 'all') -> List[Dict]:
        """
        Get users with most 'Added to Wallet' credits for a time period.
        period: 'all' | 'week' | 'today'
        """
        date_filter = ""
        if period == 'week':
            date_filter = "AND t.created_at >= DATEADD(day, -7, CURRENT_DATE)"
        elif period == 'today':
            date_filter = "AND DATE(t.created_at) = CURRENT_DATE"
        
        query = f"""
        SELECT 
            u.user_id,
            u.user_name,
            COALESCE(SUM(t.amount), 0) as total_added
        FROM loyalty.dim_loyalty_users u
        INNER JOIN loyalty.fact_wallet_transactions t ON u.user_id = t.user_id
        WHERE t.title = 'Added to Wallet' AND t.amount > 0
        {date_filter}
        GROUP BY u.user_id, u.user_name
        ORDER BY total_added DESC
        LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    def get_top_withdrawers_by_period(self, limit: int = 10, period: str = 'all') -> List[Dict]:
        """
        Get users with most withdrawal requests for a time period.
        period: 'all' | 'week' | 'today'
        """
        date_filter = ""
        if period == 'week':
            date_filter = "AND created_at >= DATEADD(day, -7, CURRENT_DATE)"
        elif period == 'today':
            date_filter = "AND DATE(created_at) = CURRENT_DATE"
        
        query = f"""
        SELECT 
            user_id,
            user_name,
            COUNT(*) as withdrawal_count,
            COALESCE(SUM(requested_amount), 0) as total_requested
        FROM loyalty.fact_withdrawals
        WHERE user_id IS NOT NULL AND user_id != ''
        {date_filter}
        GROUP BY user_id, user_name
        ORDER BY total_requested DESC
        LIMIT %s
        """
        return self.execute_query(query, (limit,))
    
    # =========================================================================
    # REFERRAL ROI CALCULATIONS
    # =========================================================================
    
    def get_referral_program_roi(self) -> Dict:
        """
        Calculate overall Referral Program ROI using ONLY loyalty schema.
        Tables: loyalty.fact_referrals (with bonus_amount), loyalty.fact_orders
        """
        try:
            # Query 1: Total bonus coins paid for referrals
            bonus_query = """
            SELECT COALESCE(SUM(bonus_amount), 0) as total_bonus_coins
            FROM loyalty.fact_referrals
            WHERE bonus_amount IS NOT NULL AND bonus_amount > 0
            """
            bonus_result = self.execute_query(bonus_query)
            coins_spent = float(bonus_result[0].get('total_bonus_coins', 0)) if bonus_result else 0
            
            # Query 2: Referral conversion statistics
            stats_query = """
            SELECT 
                COUNT(*) as total_referrals,
                COUNT(CASE WHEN referred_user_id IS NOT NULL AND referred_user_id != '' THEN 1 END) as converted_referrals
            FROM loyalty.fact_referrals
            """
            stats_result = self.execute_query(stats_query)
            total_referrals = int(stats_result[0].get('total_referrals', 0)) if stats_result else 0
            converted_referrals = int(stats_result[0].get('converted_referrals', 0)) if stats_result else 0
            
            # Query 3: Revenue from referred users' orders (using loyalty.fact_orders)
            revenue_query = """
            SELECT COALESCE(SUM(o.grand_total), 0) as total_revenue
            FROM loyalty.fact_orders o
            INNER JOIN loyalty.fact_referrals r ON o.user_id = r.referred_user_id
            WHERE o.order_status NOT IN ('CANCELLED', 'FAILED', 'REJECTED')
              AND o.grand_total > 0
            """
            revenue_result = self.execute_query(revenue_query)
            revenue = float(revenue_result[0].get('total_revenue', 0)) if revenue_result else 0
            
            # Calculate ROI (using avg 0.5 rupee per coin based on tier rates)
            coin_cost_in_rupees = coins_spent * 0.5
            roi_multiplier = (revenue / coin_cost_in_rupees) if coin_cost_in_rupees > 0 else 0
            
            return {
                'coins_spent': coins_spent,
                'revenue_generated': revenue,
                'total_referrals': total_referrals,
                'converted_referrals': converted_referrals,
                'roi_multiplier': round(roi_multiplier, 1)
            }
        except Exception as e:
            logger.error(f"ROI calculation error: {e}")
            return {'coins_spent': 0, 'revenue_generated': 0, 'total_referrals': 0, 'converted_referrals': 0, 'roi_multiplier': 0}
    
    # =========================================================================
    # SUMMARY STATISTICS
    # =========================================================================
    
    def get_loyalty_summary(self) -> Dict:
        """Get overall loyalty program statistics."""
        query = """
        SELECT 
            (SELECT COUNT(*) FROM loyalty.dim_loyalty_users) as total_users,
            (SELECT COUNT(*) FROM loyalty.dim_loyalty_users WHERE remaining_coins > 0) as active_users,
            (SELECT COALESCE(SUM(remaining_coins), 0) FROM loyalty.dim_loyalty_users) as total_coins,
            (SELECT COUNT(*) FROM loyalty.fact_referrals) as total_referrals,
            (SELECT COUNT(*) FROM loyalty.fact_leads) as total_leads,
            (SELECT COUNT(*) FROM loyalty.fact_withdrawals WHERE status = 'Pending') as pending_withdrawals
        """
        results = self.execute_query(query)
        return results[0] if results else {}
    
    # =========================================================================
    # ORDER HISTORY QUERIES
    # =========================================================================
    
    def get_all_orders(self, limit: int = 100, offset: int = 0, period: str = 'all', 
                       start_date: date = None, end_date: date = None, 
                       search_query: str = None) -> List[Dict]:
        """
        Get orders from fact_orders with filtering and pagination.
        
        Args:
            limit: Number of orders to return
            offset: Offset for pagination
            period: 'all' | 'month' | 'today' | 'custom'
            start_date: Custom start date (for period='custom')
            end_date: Custom end date (for period='custom')
            search_query: Search by user_name, phone_number, or order_id
        """
        # Build date filter
        date_filter = ""
        if period == 'today':
            date_filter = "AND DATE(o.created_at) = CURRENT_DATE"
        elif period == 'month':
            date_filter = "AND o.created_at >= DATEADD(day, -30, CURRENT_DATE)"
        elif period == 'custom' and start_date and end_date:
            date_filter = f"AND DATE(o.created_at) BETWEEN '{start_date}' AND '{end_date}'"
        
        # Build search filter
        search_filter = ""
        if search_query and search_query.strip():
            safe_query = search_query.replace("'", "''").strip()
            search_filter = f"""
            AND (
                LOWER(o.user_name) LIKE LOWER('%{safe_query}%')
                OR o.phone_number LIKE '%{safe_query}%'
                OR LOWER(o.order_id) LIKE LOWER('%{safe_query}%')
            )
            """
        
        query = f"""
        SELECT 
            o.order_id,
            o.user_id,
            o.user_name,
            o.phone_number,
            COALESCE(o.grand_total, 0) as grand_total,
            COALESCE(o.sub_total, 0) as sub_total,
            COALESCE(o.discount, 0) as discount,
            COALESCE(o.coins_used, 0) as coins_used,
            o.order_status,
            o.payment_mode,
            o.created_at
        FROM loyalty.fact_orders o
        WHERE 1=1
        {date_filter}
        {search_filter}
        ORDER BY o.created_at DESC
        LIMIT {limit} OFFSET {offset}
        """
        return self.execute_query(query)
    
    def get_orders_count(self, period: str = 'all', start_date: date = None, 
                         end_date: date = None, search_query: str = None) -> int:
        """Get total count of orders for pagination."""
        date_filter = ""
        if period == 'today':
            date_filter = "AND DATE(created_at) = CURRENT_DATE"
        elif period == 'month':
            date_filter = "AND created_at >= DATEADD(day, -30, CURRENT_DATE)"
        elif period == 'custom' and start_date and end_date:
            date_filter = f"AND DATE(created_at) BETWEEN '{start_date}' AND '{end_date}'"
        
        search_filter = ""
        if search_query and search_query.strip():
            safe_query = search_query.replace("'", "''").strip()
            search_filter = f"""
            AND (
                LOWER(user_name) LIKE LOWER('%{safe_query}%')
                OR phone_number LIKE '%{safe_query}%'
                OR LOWER(order_id) LIKE LOWER('%{safe_query}%')
            )
            """
        
        query = f"""
        SELECT COUNT(*) as total
        FROM loyalty.fact_orders
        WHERE 1=1
        {date_filter}
        {search_filter}
        """
        results = self.execute_query(query)
        return int(results[0]['total']) if results else 0
    
    def get_order_stats(self) -> Dict:
        """Get order statistics for summary cards."""
        query = """
        SELECT 
            COUNT(*) as total_orders,
            COALESCE(SUM(grand_total), 0) as total_revenue,
            COUNT(CASE WHEN DATE(created_at) = CURRENT_DATE THEN 1 END) as today_orders,
            COALESCE(SUM(CASE WHEN DATE(created_at) = CURRENT_DATE THEN grand_total ELSE 0 END), 0) as today_revenue
        FROM loyalty.fact_orders
        WHERE order_status NOT IN ('CANCELLED', 'FAILED', 'REJECTED')
        """
        results = self.execute_query(query)
        return results[0] if results else {'total_orders': 0, 'total_revenue': 0, 'today_orders': 0, 'today_revenue': 0}


# Singleton instance
_redshift_service = None

def get_redshift_service() -> RedshiftService:
    """Get or create singleton RedshiftService instance."""
    global _redshift_service
    if _redshift_service is None:
        _redshift_service = RedshiftService()
    return _redshift_service


# Convenience functions for direct import
def get_coins_by_tier():
    return get_redshift_service().get_coins_by_tier()

def get_total_coins_in_system():
    return get_redshift_service().get_total_coins_in_system()

def get_active_users_count():
    return get_redshift_service().get_active_users_count()

def get_top_coin_holders(limit=10):
    return get_redshift_service().get_top_coin_holders(limit)

def get_top_earners(limit=10):
    return get_redshift_service().get_top_earners(limit)

def get_top_referrers(limit=10):
    return get_redshift_service().get_top_referrers(limit)

def get_top_lead_generators(limit=10):
    return get_redshift_service().get_top_lead_generators(limit)

def get_top_withdrawers(limit=10):
    return get_redshift_service().get_top_withdrawers(limit)

def get_daily_coin_activity(days=30):
    return get_redshift_service().get_daily_coin_activity(days)

def get_daily_coin_activity_by_range(start_date, end_date):
    return get_redshift_service().get_daily_coin_activity_by_range(start_date, end_date)

def get_top_added_to_wallet(limit=10):
    return get_redshift_service().get_top_added_to_wallet(limit)

def get_referral_stats_by_range(start_date, end_date):
    return get_redshift_service().get_referral_stats_by_range(start_date, end_date)

# Time-filtered leaderboard functions
def get_top_referrers_by_period(limit=10, period='all'):
    return get_redshift_service().get_top_referrers_by_period(limit, period)

def get_top_lead_generators_by_period(limit=10, period='all'):
    return get_redshift_service().get_top_lead_generators_by_period(limit, period)

def get_top_earners_by_period(limit=10, period='all'):
    return get_redshift_service().get_top_earners_by_period(limit, period)

def get_top_added_to_wallet_by_period(limit=10, period='all'):
    return get_redshift_service().get_top_added_to_wallet_by_period(limit, period)

def get_top_withdrawers_by_period(limit=10, period='all'):
    return get_redshift_service().get_top_withdrawers_by_period(limit, period)

def get_referral_program_roi():
    return get_redshift_service().get_referral_program_roi()

# Order history functions
def get_all_orders(limit=100, offset=0, period='all', start_date=None, end_date=None, search_query=None):
    return get_redshift_service().get_all_orders(limit, offset, period, start_date, end_date, search_query)

def get_orders_count(period='all', start_date=None, end_date=None, search_query=None):
    return get_redshift_service().get_orders_count(period, start_date, end_date, search_query)

def get_order_stats():
    return get_redshift_service().get_order_stats()
