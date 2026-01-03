"""
Lead Service - Lead operations from LeadTable
"""
from typing import List, Dict
from datetime import datetime, date
from .dynamodb_service import db_service
from . import aggregates_service


TABLE_NAME = "LeadTable"


def get_leads_by_user(user_id: str) -> List[Dict]:
    """Get all leads for a user."""
    return db_service.query_by_index(
        TABLE_NAME,
        "userIndex",
        "userId",
        user_id
    )


def get_all_leads(limit: int = 100) -> List[Dict]:
    """Get all leads."""
    return db_service.scan_all(TABLE_NAME, limit)


def get_top_lead_generators(limit: int = 5) -> List[Dict]:
    """
    G03: Get users who generated the most leads.
    Returns list of {userId, userName, leadCount} sorted DESC.
    """
    # Try aggregates first
    cached = aggregates_service.get_top_lead_generators_from_aggregates(limit)
    if cached:
        return cached
    
    # Fallback to full scan
    from .user_service import get_user_by_id
    from collections import Counter
    
    leads = get_all_leads(limit=None)
    
    # Count leads per user
    user_counts = Counter(l.get('userId', '') for l in leads if l.get('userId'))
    
    # Get top users
    top_users = user_counts.most_common(limit)
    
    result = []
    for user_id, count in top_users:
        user = get_user_by_id(user_id)
        user_name = user.get('userName', 'Unknown') if user else 'Unknown'
        
        result.append({
            'userId': user_id,
            'userName': user_name,
            'leadCount': count
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


def get_today_leads_count() -> int:
    """Count leads created today."""
    # NOTE: Aggregates cache disabled - today's count must be live
    # The cached value from seed_aggregates.py becomes stale immediately
    # cached = aggregates_service.get_today_leads_from_aggregates()
    # if cached is not None:
    #     return cached
    
    # Live count from LeadTable
    try:
        leads = get_all_leads(limit=None)
        today = date.today().isoformat()
        
        count = 0
        for lead in leads:
            created = _parse_date(lead.get('created_time'))
            if created == today:
                count += 1
        
        return count
    except Exception as e:
        print(f"Error getting today's leads: {e}")
        return 0


def get_today_leads() -> List[Dict]:
    """Get all leads created today (returns full records)."""
    try:
        leads = get_all_leads(limit=None)
        today = date.today().isoformat()
        
        return [lead for lead in leads if _parse_date(lead.get('created_time')) == today]
    except Exception as e:
        print(f"Error getting today's leads: {e}")
        return []

