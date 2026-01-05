"""
Aggregates Service - Read pre-computed metrics from AdminAggregatesTable

This service reads from the aggregates table populated by Lambda/Streams.
Falls back to live calculation if aggregates are unavailable.
"""
from typing import Dict, List, Optional
from datetime import datetime, date
from decimal import Decimal
from .dynamodb_service import db_service, convert_decimals

# Aggregates Table name - create this table in AWS Console
AGGREGATES_TABLE = "AdminAggregatesTable"

# Feature flag - set to True once Lambda is deployed and aggregates are populated
USE_AGGREGATES = True  # Enabled! Aggregates table has been seeded


def _get_aggregate(aggregate_type: str, aggregate_id: str) -> Optional[Dict]:
    """
    Get a single aggregate record from AdminAggregatesTable.
    
    Args:
        aggregate_type: GLOBAL, TIER, LEADERBOARD, or DAILY
        aggregate_id: Specific ID within the type
    
    Returns:
        The aggregate record or None if not found
    """
    try:
        result = db_service.get_item(
            AGGREGATES_TABLE,
            {
                "aggregateType": aggregate_type,
                "aggregateId": aggregate_id
            }
        )
        return result
    except Exception as e:
        print(f"[WARN] Could not fetch aggregate {aggregate_type}/{aggregate_id}: {e}")
        return None


def _put_aggregate(aggregate_type: str, aggregate_id: str, data: Dict) -> bool:
    """
    Write an aggregate record to AdminAggregatesTable.
    Used by seed script and Lambda.
    
    Args:
        aggregate_type: GLOBAL, TIER, LEADERBOARD, or DAILY
        aggregate_id: Specific ID within the type
        data: The aggregate data to store
    
    Returns:
        True if successful, False otherwise
    """
    try:
        table = db_service.get_table(AGGREGATES_TABLE)
        
        # Convert floats to Decimal for DynamoDB
        def convert_to_decimal(obj):
            if isinstance(obj, float):
                return Decimal(str(obj))
            elif isinstance(obj, dict):
                return {k: convert_to_decimal(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [convert_to_decimal(i) for i in obj]
            return obj
        
        item = {
            "aggregateType": aggregate_type,
            "aggregateId": aggregate_id,
            "lastUpdated": int(datetime.now().timestamp()),
            "data": convert_to_decimal(data)
        }
        
        table.put_item(Item=item)
        return True
    except Exception as e:
        print(f"[ERROR] Could not write aggregate {aggregate_type}/{aggregate_id}: {e}")
        return False


# ============ GLOBAL KPIs ============

# Cache aggregates for 60 seconds to reduce DynamoDB calls
from functools import lru_cache
import time

_cache = {}
_cache_ttl = 60  # seconds

def _get_cached_global_stats():
    """Get global stats with caching."""
    cache_key = "global_stats"
    now = time.time()
    
    if cache_key in _cache:
        data, timestamp = _cache[cache_key]
        if now - timestamp < _cache_ttl:
            return data
    
    # Fetch fresh data
    if not USE_AGGREGATES:
        return {}
    
    agg = _get_aggregate("GLOBAL", "STATS")
    if agg and "data" in agg:
        data = convert_decimals(agg["data"])
        _cache[cache_key] = (data, now)
        return data
    return {}


def get_global_stats() -> Dict:
    """
    Get all global KPIs from aggregates.
    
    Returns:
        {
            'totalCoins': float,
            'activeUsersCount': int,
            'pendingWithdrawalsCount': int,
            'pendingWithdrawalsAmount': float,
            'todayReferralsCount': int,
            'todayLeadsCount': int
        }
    """
    return _get_cached_global_stats()


def get_total_coins_from_aggregates() -> Optional[float]:
    """Get total coins from aggregates. Returns None if unavailable."""
    stats = get_global_stats()
    return stats.get('totalCoins') if stats else None


def get_active_users_from_aggregates() -> Optional[int]:
    """Get active users count from aggregates. Returns None if unavailable."""
    stats = get_global_stats()
    return stats.get('activeUsersCount') if stats else None


def get_pending_count_from_aggregates() -> Optional[int]:
    """Get pending withdrawals count from aggregates. Returns None if unavailable."""
    stats = get_global_stats()
    return stats.get('pendingWithdrawalsCount') if stats else None


def get_today_referrals_from_aggregates() -> Optional[int]:
    """Get today's referrals count from DAILY aggregates (not stale GLOBAL)."""
    today = date.today().isoformat()
    agg = _get_aggregate("DAILY", today)
    if agg and "data" in agg:
        data = convert_decimals(agg["data"])
        return data.get('referrals', 0)
    return 0  # Return 0 if no data for today yet


def get_today_leads_from_aggregates() -> Optional[int]:
    """Get today's leads count from DAILY aggregates (not stale GLOBAL)."""
    today = date.today().isoformat()
    agg = _get_aggregate("DAILY", today)
    if agg and "data" in agg:
        data = convert_decimals(agg["data"])
        return data.get('leads', 0)
    return 0  # Return 0 if no data for today yet


# ============ TIER STATISTICS ============

def get_tier_stats_from_aggregates() -> Dict[str, Dict]:
    """
    Get tier-wise statistics from aggregates (cached).
    """
    cache_key = "tier_stats"
    now = time.time()
    
    if cache_key in _cache:
        data, timestamp = _cache[cache_key]
        if now - timestamp < _cache_ttl:
            return data
    
    if not USE_AGGREGATES:
        return {}
    
    tier_stats = {}
    for tier in ['Gold', 'Silver', 'Bronze', 'Unknown']:
        agg = _get_aggregate("TIER", tier)
        if agg and "data" in agg:
            tier_stats[tier] = convert_decimals(agg["data"])
    
    _cache[cache_key] = (tier_stats, now)
    return tier_stats


# ============ LEADERBOARDS ============

def get_leaderboard(leaderboard_name: str) -> List[Dict]:
    """
    Get a leaderboard from aggregates (cached).
    """
    cache_key = f"leaderboard_{leaderboard_name}"
    now = time.time()
    
    if cache_key in _cache:
        data, timestamp = _cache[cache_key]
        if now - timestamp < _cache_ttl:
            return data
    
    if not USE_AGGREGATES:
        return []
    
    agg = _get_aggregate("LEADERBOARD", leaderboard_name)
    if agg and "data" in agg:
        data = convert_decimals(agg["data"])
        items = data.get("items", [])
        _cache[cache_key] = (items, now)
        return items
    return []


def get_weekly_leaderboard(leaderboard_name: str, limit: int = 5) -> List[Dict]:
    """
    Get weekly leaderboard from WEEKLY_LEADERBOARD aggregates.
    Returns list of {userId, userName, count} sorted by count DESC.
    
    The weekly leaderboard stores user IDs and counts.
    We need to look up user names.
    """
    from .user_service import get_user_by_id
    
    cache_key = f"weekly_leaderboard_{leaderboard_name}"
    now = time.time()
    
    if cache_key in _cache:
        data, timestamp = _cache[cache_key]
        if now - timestamp < _cache_ttl:
            return data[:limit]
    
    if not USE_AGGREGATES:
        return []
    
    agg = _get_aggregate("WEEKLY_LEADERBOARD", leaderboard_name)
    if agg and "data" in agg:
        data = convert_decimals(agg["data"])
        users_dict = data.get("users", {})
        
        # Sort by count descending
        sorted_users = sorted(users_dict.items(), key=lambda x: int(x[1]), reverse=True)
        
        # Build result with user names
        result = []
        for user_id, count in sorted_users[:limit]:
            user = get_user_by_id(user_id)
            user_name = user.get('userName', 'Unknown') if user else 'Unknown'
            result.append({
                'userId': user_id,
                'userName': user_name,
                'count': int(count)
            })
        
        _cache[cache_key] = (result, now)
        return result
    
    return []


def get_top_coin_holders_from_aggregates(limit: int = 5) -> List[Dict]:
    """Get top coin holders (gains) from weekly aggregates (falls back to all-time if unavailable)."""
    # Try weekly first
    weekly = get_weekly_leaderboard("TOP_COIN_HOLDERS", limit)
    if weekly:
        # Convert to expected format (weekly tracks gains, stored as coin amount)
        return [{'userId': u['userId'], 'userName': u['userName'], 'coins': u['count']} for u in weekly]
    
    # Fallback to all-time (from LEADERBOARD type)
    items = get_leaderboard("TOP_COIN_HOLDERS")
    return items[:limit] if items else []


def get_top_referrers_from_aggregates(limit: int = 5) -> List[Dict]:
    """Get top referrers from weekly aggregates (falls back to all-time if unavailable)."""
    # Try weekly first
    weekly = get_weekly_leaderboard("TOP_REFERRERS", limit)
    if weekly:
        # Convert to expected format
        return [{'userId': u['userId'], 'userName': u['userName'], 'referralCount': u['count']} for u in weekly]
    
    # Fallback to all-time (from LEADERBOARD type)
    items = get_leaderboard("TOP_REFERRERS")
    return items[:limit] if items else []


def get_top_lead_generators_from_aggregates(limit: int = 5) -> List[Dict]:
    """Get top lead generators from weekly aggregates (falls back to all-time if unavailable)."""
    # Try weekly first
    weekly = get_weekly_leaderboard("TOP_LEAD_GENERATORS", limit)
    if weekly:
        # Convert to expected format
        return [{'userId': u['userId'], 'userName': u['userName'], 'leadCount': u['count']} for u in weekly]
    
    # Fallback to all-time (from LEADERBOARD type)
    items = get_leaderboard("TOP_LEAD_GENERATORS")
    return items[:limit] if items else []


def get_top_earners_from_aggregates(limit: int = 5) -> List[Dict]:
    """Get top earners from aggregates (all-time for now - needs WalletTransactionTable stream)."""
    items = get_leaderboard("TOP_EARNERS")
    return items[:limit] if items else []


def get_top_withdrawers_from_aggregates(limit: int = 5) -> List[Dict]:
    """Get top withdrawers from weekly aggregates (falls back to all-time if unavailable)."""
    from .user_service import get_user_by_id
    
    # Try weekly first
    agg = _get_aggregate("WEEKLY_LEADERBOARD", "TOP_WITHDRAWERS")
    if agg and "data" in agg:
        data = convert_decimals(agg["data"])
        users_dict = data.get("users", {})
        
        # Sort by count descending
        sorted_users = sorted(users_dict.items(), key=lambda x: int(x[1].get('count', 0)) if isinstance(x[1], dict) else 1, reverse=True)
        
        result = []
        for user_id, user_data in sorted_users[:limit]:
            user = get_user_by_id(user_id)
            user_name = user.get('userName', 'Unknown') if user else 'Unknown'
            if isinstance(user_data, dict):
                result.append({
                    'userId': user_id,
                    'userName': user_name,
                    'withdrawalCount': int(user_data.get('count', 1)),
                    'totalAmount': float(user_data.get('amount', 0))
                })
            else:
                result.append({
                    'userId': user_id,
                    'userName': user_name,
                    'withdrawalCount': 1,
                    'totalAmount': float(user_data)
                })
        
        if result:
            return result
    
    # Fallback to all-time (from LEADERBOARD type)
    items = get_leaderboard("TOP_WITHDRAWERS")
    return items[:limit] if items else []


# ============ DAILY METRICS ============

def get_daily_metrics(start_date: date, end_date: date) -> List[Dict]:
    """
    Get daily metrics for a date range from aggregates.
    
    Returns:
        [
            {'date': '2026-01-01', 'referrals': X, 'leads': Y, 'coinCredits': Z, 'coinDebits': W},
            ...
        ]
    """
    if not USE_AGGREGATES:
        return []
    
    results = []
    current = start_date
    from datetime import timedelta
    
    while current <= end_date:
        date_str = current.isoformat()
        agg = _get_aggregate("DAILY", date_str)
        if agg and "data" in agg:
            data = convert_decimals(agg["data"])
            results.append({
                'date': date_str,
                **data
            })
        else:
            # Return empty data for missing dates
            results.append({
                'date': date_str,
                'referrals': 0,
                'leads': 0,
                'coinCredits': 0,
                'coinDebits': 0
            })
        current += timedelta(days=1)
    
    return results


# ============ HELPER: Check if aggregates are available ============

def is_aggregates_enabled() -> bool:
    """Check if aggregates are enabled and working."""
    if not USE_AGGREGATES:
        return False
    
    # Try to fetch global stats to verify table exists
    try:
        agg = _get_aggregate("GLOBAL", "STATS")
        return agg is not None
    except:
        return False
