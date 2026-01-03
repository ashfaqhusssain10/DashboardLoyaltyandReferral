"""
Referral Service - Referral operations from TierReferralTable
"""
from typing import List, Dict
from datetime import datetime, date, timedelta
from .dynamodb_service import db_service
from . import aggregates_service


TABLE_NAME = "TierReferralTable"


def get_referrals_by_user(user_id: str) -> List[Dict]:
    """Get all referrals made by a user."""
    return db_service.query_by_index(
        TABLE_NAME,
        "userIdIndex",
        "userId",
        user_id
    )


def get_all_referrals(limit: int = 100) -> List[Dict]:
    """Get all referrals."""
    return db_service.scan_all(TABLE_NAME, limit)


def get_top_referrers(limit: int = 5) -> List[Dict]:
    """
    G02: Get users who referred the most people.
    Returns list of {userId, userName, referralCount} sorted DESC.
    """
    # Try aggregates first
    cached = aggregates_service.get_top_referrers_from_aggregates(limit)
    if cached:
        return cached
    
    # Fallback to full scan
    from .user_service import get_user_by_id
    from collections import Counter
    
    referrals = get_all_referrals(limit=None)
    
    # Count referrals per user
    user_counts = Counter(r.get('userId', '') for r in referrals if r.get('userId'))
    
    # Get top users
    top_users = user_counts.most_common(limit)
    
    result = []
    for user_id, count in top_users:
        user = get_user_by_id(user_id)
        user_name = user.get('userName', 'Unknown') if user else 'Unknown'
        
        result.append({
            'userId': user_id,
            'userName': user_name,
            'referralCount': count
        })
    
    return result

def _parse_date(created_time) -> str:
    """Parse created_time to date string, handling various formats."""
    if created_time is None:
        return ""
    
    # If it's an integer (timestamp in milliseconds)
    if isinstance(created_time, (int, float)):
        try:
            # Assume milliseconds timestamp
            if created_time > 10000000000:
                created_time = created_time / 1000
            dt = datetime.fromtimestamp(created_time)
            return dt.date().isoformat()
        except:
            return ""
    
    # If it's a string
    if isinstance(created_time, str):
        return created_time[:10]  # Get date part
    
    return ""


def get_today_referrals_count() -> int:
    """Count referrals created today."""
    # Try aggregates first
    cached = aggregates_service.get_today_referrals_from_aggregates()
    if cached is not None:
        return cached
    
    # Fallback to full scan
    try:
        referrals = get_all_referrals(limit=None)
        today = date.today().isoformat()
        
        count = 0
        for ref in referrals:
            created = _parse_date(ref.get('created_time'))
            if created == today:
                count += 1
        
        return count
    except Exception as e:
        print(f"Error getting today's referrals: {e}")
        return 0


def get_weekly_referral_stats() -> List[Dict]:
    """Get referral counts for last 7 days."""
    try:
        referrals = get_all_referrals(limit=None)
        
        # Initialize last 7 days
        stats = {}
        for i in range(7):
            day = date.today() - timedelta(days=i)
            stats[day.isoformat()] = 0
        
        # Count referrals per day
        for ref in referrals:
            created = _parse_date(ref.get('created_time'))
            if created in stats:
                stats[created] += 1
        
        # Convert to list for chart
        result = [
            {"date": day, "count": count}
            for day, count in sorted(stats.items())
        ]
        
        return result
    except Exception as e:
        print(f"Error getting weekly stats: {e}")
        return []


def get_referral_stats_by_range(start_date: date, end_date: date) -> List[Dict]:
    """Get referral counts between start_date and end_date."""
    try:
        referrals = get_all_referrals(limit=2000)
        
        # Initialize date range
        stats = {}
        current = start_date
        while current <= end_date:
            stats[current.isoformat()] = 0
            current += timedelta(days=1)
        
        # Count referrals per day
        for ref in referrals:
            created = _parse_date(ref.get('created_time'))
            if created in stats:
                stats[created] += 1
        
        # Convert to list for chart
        result = [
            {"date": day, "count": count}
            for day, count in sorted(stats.items())
        ]
        
        return result
    except Exception as e:
        print(f"Error getting referral stats by range: {e}")
        return []


def get_today_referrals() -> List[Dict]:
    """Get all referrals created today (returns full records)."""
    try:
        referrals = get_all_referrals(limit=None)
        today = date.today().isoformat()
        
        return [ref for ref in referrals if _parse_date(ref.get('created_time')) == today]
    except Exception as e:
        print(f"Error getting today's referrals: {e}")
        return []

