"""
Wallet Service - Wallet and transaction operations
"""
from typing import Optional, List, Dict
from .dynamodb_service import db_service
from . import aggregates_service


WALLET_TABLE = "WalletTable"
TRANSACTION_TABLE = "WalletTransactionTable"

# Tier-specific Coin to Rupee conversion rates
TIER_CONVERSION_RATES = {
    'Bronze': 0.40,
    'Silver': 0.70,
    'Gold': 1.00,
    'Unknown': 0.40  # Default rate
}


def get_tier_rate(tier_name: str) -> float:
    """Get conversion rate for a tier."""
    return TIER_CONVERSION_RATES.get(tier_name, 0.40)


def get_wallet_by_user(user_id: str) -> Optional[Dict]:
    """Get wallet for a user."""
    results = db_service.query_by_index(
        WALLET_TABLE,
        "userIndex",
        "userId",
        user_id
    )
    return results[0] if results else None


def get_all_wallets(limit: int = 100) -> List[Dict]:
    """Get all wallets."""
    return db_service.scan_all(WALLET_TABLE, limit)


def get_transactions_by_user(user_id: str) -> List[Dict]:
    """Get all transactions for a user."""
    return db_service.query_by_index(
        TRANSACTION_TABLE,
        "userIndex",
        "userId",
        user_id
    )


def get_all_transactions(limit: int = 100) -> List[Dict]:
    """Get all transactions."""
    return db_service.scan_all(TRANSACTION_TABLE, limit)


def get_total_coins_in_system() -> float:
    """Calculate total coins across all wallets."""
    # Try aggregates first
    cached = aggregates_service.get_total_coins_from_aggregates()
    if cached is not None:
        return cached
    
    # Fallback to full scan
    wallets = get_all_wallets(limit=None)  # Full pagination for accurate count
    total = sum(float(w.get('remainingAmount', 0)) for w in wallets)
    return total


def get_active_users_count() -> int:
    """Count users with coin balance > 0 (users with redeemable balance)."""
    # Try aggregates first
    cached = aggregates_service.get_active_users_from_aggregates()
    if cached is not None:
        return cached
    
    # Fallback to full scan
    wallets = get_all_wallets(limit=None)  # Full pagination for accurate count
    active = sum(1 for w in wallets if float(w.get('remainingAmount', 0)) > 0)
    return active


def get_coins_by_tier() -> Dict[str, Dict]:
    """
    Get total coins grouped by tier with rupee value.
    Uses tier-specific conversion rates:
    - Bronze: 0.40
    - Silver: 0.70
    - Gold: 1.00
    Returns: {tier_name: {'coins': X, 'rupees': Y, 'users': Z, 'rate': R}}
    """
    # Try aggregates first
    cached = aggregates_service.get_tier_stats_from_aggregates()
    if cached:
        return cached
    
    # Fallback to full scan
    from .user_service import get_user_by_id
    from .tier_service import get_tier_name
    
    wallets = get_all_wallets(limit=None)  # Full pagination for accurate tier counts
    tier_stats = {}
    
    for wallet in wallets:
        user_id = wallet.get('userId', '')
        coins = float(wallet.get('remainingAmount', 0))
        
        if coins <= 0:
            continue
        
        # Get user's tier
        user = get_user_by_id(user_id)
        tier_id = user.get('tierId', 'unknown') if user else 'unknown'
        tier_name = get_tier_name(tier_id) if tier_id != 'unknown' else 'Unknown'
        
        # Get tier-specific rate
        rate = get_tier_rate(tier_name)
        
        if tier_name not in tier_stats:
            tier_stats[tier_name] = {'coins': 0, 'rupees': 0, 'users': 0, 'rate': rate}
        
        tier_stats[tier_name]['coins'] += coins
        tier_stats[tier_name]['rupees'] += coins * rate
        tier_stats[tier_name]['users'] += 1
    
    return tier_stats


def coins_to_rupees(coins: float, tier_name: str = 'Bronze') -> float:
    """Convert coins to rupees based on tier rate."""
    rate = get_tier_rate(tier_name)
    return coins * rate


def get_top_coin_holders(limit: int = 5) -> List[Dict]:
    """
    G01: Get users with highest coin balance.
    Returns list of {userId, userName, coins} sorted by coins DESC.
    """
    # Try aggregates first
    cached = aggregates_service.get_top_coin_holders_from_aggregates(limit)
    if cached:
        return cached
    
    # Fallback to full scan
    from .user_service import get_user_by_id
    
    wallets = get_all_wallets(limit=None)  # Full pagination for accurate leaderboard
    
    # Sort by remaining amount
    sorted_wallets = sorted(
        wallets, 
        key=lambda w: float(w.get('remainingAmount', 0)), 
        reverse=True
    )[:limit]
    
    result = []
    for wallet in sorted_wallets:
        user_id = wallet.get('userId', '')
        coins = float(wallet.get('remainingAmount', 0))
        
        if coins <= 0:
            continue
            
        user = get_user_by_id(user_id)
        user_name = user.get('userName', 'Unknown') if user else 'Unknown'
        
        result.append({
            'userId': user_id,
            'userName': user_name,
            'coins': coins
        })
    
    return result[:limit]


def get_top_earners(limit: int = 5) -> List[Dict]:
    """
    G04: Get users who earned the most coins (total credits from earning transactions).
    Filters by specific earning titles: Signup Bonus, Referral, etc.
    Excludes: Users who have 'Added to Wallet' transactions, Refunds, Withdrawals, Redemptions.
    Returns list of {userId, userName, totalEarned} sorted DESC.
    """
    # NOTE: Aggregates cache disabled - we need custom logic to exclude 'Added to Wallet' users
    # cached = aggregates_service.get_top_earners_from_aggregates(limit)
    # if cached:
    #     return cached
    
    # Fallback to full scan
    from .user_service import get_user_by_id
    from collections import defaultdict
    
    # Define earning titles (case-insensitive matching)
    # Note: 'Added to Wallet' is tracked separately in get_top_added_to_wallet()
    EARNING_TITLES = {
        'signup bonus',
        'referral',
        'referral reward',
        'referral order completed',
        'referral order is completed',
        'loyalty cashback'
    }
    
    transactions = get_all_transactions(limit=None)  # Full pagination for accurate leaderboard
    
    # First pass: identify users who have 'Added to Wallet' transactions
    users_with_added_to_wallet = set()
    for txn in transactions:
        title = str(txn.get('title', '')).lower().strip()
        if title == 'added to wallet':
            users_with_added_to_wallet.add(txn.get('userId', ''))
    
    # Second pass: sum credits per user for earning transactions only, excluding users with 'Added to Wallet'
    user_credits = defaultdict(float)
    for txn in transactions:
        title = str(txn.get('title', '')).lower().strip()
        user_id = txn.get('userId', '')
        
        # Skip users who have 'Added to Wallet' transactions
        if user_id in users_with_added_to_wallet:
            continue
            
        if title in EARNING_TITLES:
            amount = abs(float(txn.get('amount', 0)))
            user_credits[user_id] += amount
    
    # Sort and get top users
    sorted_users = sorted(user_credits.items(), key=lambda x: x[1], reverse=True)[:limit]
    
    result = []
    for user_id, total_earned in sorted_users:
        user = get_user_by_id(user_id)
        user_name = user.get('userName', 'Unknown') if user else 'Unknown'
        
        result.append({
            'userId': user_id,
            'userName': user_name,
            'totalEarned': total_earned
        })
    
    return result


def get_top_added_to_wallet(limit: int = 5) -> List[Dict]:
    """
    Get users with highest 'Added to Wallet' credits (verified referral bonuses).
    This tracks users who received the most referral bonus credits.
    Returns list of {userId, userName, totalAdded} sorted DESC.
    """
    from .user_service import get_user_by_id
    from collections import defaultdict
    
    transactions = get_all_transactions(limit=None)  # Full pagination
    
    # Sum 'Added to Wallet' credits per user
    user_added = defaultdict(float)
    for txn in transactions:
        title = str(txn.get('title', '')).lower().strip()
        if title == 'added to wallet':
            user_id = txn.get('userId', '')
            amount = abs(float(txn.get('amount', 0)))
            user_added[user_id] += amount
    
    # Sort and get top users
    sorted_users = sorted(user_added.items(), key=lambda x: x[1], reverse=True)[:limit]
    
    result = []
    for user_id, total_added in sorted_users:
        user = get_user_by_id(user_id)
        user_name = user.get('userName', 'Unknown') if user else 'Unknown'
        
        result.append({
            'userId': user_id,
            'userName': user_name,
            'totalAdded': total_added
        })
    
    return result


def get_daily_coin_activity(days: int = 7) -> List[Dict]:
    """
    G05: Get daily coin credits and debits for the last N days.
    Returns list of {date, credits, debits} for charting.
    """
    from datetime import date, timedelta, datetime
    from collections import defaultdict
    
    transactions = get_all_transactions(limit=2000)
    
    # Initialize stats for last N days
    daily_stats = {}
    for i in range(days):
        day = date.today() - timedelta(days=i)
        daily_stats[day.isoformat()] = {'credits': 0, 'debits': 0}
    
    # Helper to parse date
    def parse_date(created_time):
        if created_time is None:
            return ""
        if isinstance(created_time, (int, float)):
            try:
                if created_time > 10000000000:
                    created_time = created_time / 1000
                return datetime.fromtimestamp(created_time).date().isoformat()
            except:
                return ""
        if isinstance(created_time, str):
            return created_time[:10]
        return ""
    
    # Aggregate by date
    for txn in transactions:
        txn_date = parse_date(txn.get('created_time'))
        if txn_date in daily_stats:
            amount = float(txn.get('amount', 0))
            title = str(txn.get('title', '')).lower()
            
            if 'credit' in title or amount > 0:
                daily_stats[txn_date]['credits'] += abs(amount)
            elif 'debit' in title or amount < 0:
                daily_stats[txn_date]['debits'] += abs(amount)
    
    # Convert to list sorted by date
    result = [
        {'date': d, 'credits': stats['credits'], 'debits': stats['debits']}
        for d, stats in sorted(daily_stats.items())
    ]
    
    return result


def get_daily_coin_activity_by_range(start_date, end_date) -> List[Dict]:
    """
    Get daily coin credits and debits for a custom date range.
    Returns list of {date, credits, debits} for charting.
    """
    from datetime import timedelta, datetime
    from collections import defaultdict
    
    transactions = get_all_transactions(limit=3000)
    
    # Initialize stats for date range
    daily_stats = {}
    current = start_date
    while current <= end_date:
        daily_stats[current.isoformat()] = {'credits': 0, 'debits': 0}
        current += timedelta(days=1)
    
    # Helper to parse date
    def parse_date(created_time):
        if created_time is None:
            return ""
        if isinstance(created_time, (int, float)):
            try:
                if created_time > 10000000000:
                    created_time = created_time / 1000
                return datetime.fromtimestamp(created_time).date().isoformat()
            except:
                return ""
        if isinstance(created_time, str):
            return created_time[:10]
        return ""
    
    # Aggregate by date
    for txn in transactions:
        txn_date = parse_date(txn.get('created_time'))
        if txn_date in daily_stats:
            amount = float(txn.get('amount', 0))
            title = str(txn.get('title', '')).lower()
            
            if 'credit' in title or amount > 0:
                daily_stats[txn_date]['credits'] += abs(amount)
            elif 'debit' in title or amount < 0:
                daily_stats[txn_date]['debits'] += abs(amount)
    
    # Convert to list sorted by date
    result = [
        {'date': d, 'credits': stats['credits'], 'debits': stats['debits']}
        for d, stats in sorted(daily_stats.items())
    ]
    
    return result
