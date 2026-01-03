"""
Tier Service - Tier details from TierDetailsTable
"""
from typing import Optional, List, Dict
from .dynamodb_service import db_service


TABLE_NAME = "TierDetailsTable"


def get_all_tiers() -> List[Dict]:
    """Get all tier definitions."""
    return db_service.scan_all(TABLE_NAME, limit=10)


def get_tier_by_id(tier_id: str) -> Optional[Dict]:
    """Get a specific tier by ID."""
    return db_service.get_item(TABLE_NAME, {"tierId": tier_id})


def get_tier_name(tier_id: str) -> str:
    """Get tier name from ID.
    
    Returns tier name in title case (Bronze, Silver, Gold) for consistency.
    """
    tier = get_tier_by_id(tier_id)
    if tier:
        tier_type = tier.get('tierType', 'Unknown')
        # Normalize to title case (e.g., 'BRONZE' -> 'Bronze')
        return tier_type.title() if tier_type else 'Unknown'
    return 'Unknown'

