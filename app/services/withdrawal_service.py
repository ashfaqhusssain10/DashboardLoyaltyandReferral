"""
Withdrawal Service - Withdrawal operations from WithdrawnTable

⚠️ READ-ONLY MODE: Write operations are disabled for production database migration.
   Set READ_ONLY_MODE = False to re-enable write operations after migration is complete.
"""
from typing import List, Dict, Optional
from datetime import datetime
from .dynamodb_service import db_service
from . import aggregates_service


# ⚠️ PRODUCTION SAFETY: Set to False after database migration is complete
READ_ONLY_MODE = True

TABLE_NAME = "WithdrawnTable"


def get_all_withdrawals(limit: int = 100) -> List[Dict]:
    """Get all withdrawal requests."""
    return db_service.scan_all(TABLE_NAME, limit)


def get_withdrawals_by_user(user_id: str) -> List[Dict]:
    """Get withdrawals for a specific user."""
    return db_service.query_by_index(
        TABLE_NAME,
        "userIndex",
        "userId",
        user_id
    )


def get_pending_withdrawals() -> List[Dict]:
    """Get all pending withdrawal requests."""
    all_withdrawals = get_all_withdrawals(limit=None)
    return [w for w in all_withdrawals if w.get('status', '').lower() == 'pending']


def get_pending_count() -> int:
    """Count pending withdrawals."""
    # Try aggregates first
    cached = aggregates_service.get_pending_count_from_aggregates()
    if cached is not None:
        return cached
    
    # Fallback to full scan
    return len(get_pending_withdrawals())


def get_total_pending_amount() -> float:
    """Total amount in pending withdrawals."""
    pending = get_pending_withdrawals()
    return sum(float(w.get('requestedAmount', 0)) for w in pending)


def update_withdrawal_status(request_id: str, new_status: str) -> Optional[Dict]:
    """Update the status of a withdrawal request.
    
    ⚠️ DISABLED: Write operations are blocked in READ_ONLY_MODE.
    """
    if READ_ONLY_MODE:
        print(f"⚠️ BLOCKED: Write operation disabled (READ_ONLY_MODE=True). "
              f"Attempted to update withdrawal {request_id} to {new_status}")
        return None
    
    return db_service.update_item(
        TABLE_NAME,
        {"requestedId": request_id},
        "SET #status = :status, updated_time = :time",
        {
            ":status": new_status,
            ":time": datetime.now().isoformat()
        }
    )


def approve_withdrawal(request_id: str) -> Optional[Dict]:
    """Approve a withdrawal request.
    
    ⚠️ DISABLED: Write operations are blocked in READ_ONLY_MODE.
    """
    if READ_ONLY_MODE:
        print(f"⚠️ BLOCKED: approve_withdrawal disabled (READ_ONLY_MODE=True)")
        return None
    return update_withdrawal_status(request_id, "Approved")


def reject_withdrawal(request_id: str) -> Optional[Dict]:
    """Reject a withdrawal request.
    
    ⚠️ DISABLED: Write operations are blocked in READ_ONLY_MODE.
    """
    if READ_ONLY_MODE:
        print(f"⚠️ BLOCKED: reject_withdrawal disabled (READ_ONLY_MODE=True)")
        return None
    return update_withdrawal_status(request_id, "Rejected")


def get_top_withdrawers(limit: int = 5) -> List[Dict]:
    """
    Get users with most withdrawal requests.
    Returns list of {userId, userName, withdrawalCount, totalAmount} sorted DESC.
    """
    # Try aggregates first
    cached = aggregates_service.get_top_withdrawers_from_aggregates(limit)
    if cached:
        return cached
    
    # Fallback to full scan
    from .user_service import get_user_by_id
    from collections import Counter, defaultdict
    
    withdrawals = get_all_withdrawals(limit=None)
    
    # Count withdrawals and sum amounts per user
    user_counts = Counter()
    user_amounts = defaultdict(float)
    
    for w in withdrawals:
        user_id = w.get('userId', '')
        if user_id:
            user_counts[user_id] += 1
            user_amounts[user_id] += float(w.get('requestedAmount', 0))
    
    # Get top users
    top_users = user_counts.most_common(limit)
    
    result = []
    for user_id, count in top_users:
        user = get_user_by_id(user_id)
        user_name = user.get('userName', 'Unknown') if user else 'Unknown'
        
        result.append({
            'userId': user_id,
            'userName': user_name,
            'withdrawalCount': count,
            'totalAmount': user_amounts[user_id]
        })
    
    return result
