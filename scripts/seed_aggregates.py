"""
Seed Aggregates Script

Run this script once to populate AdminAggregatesTable with initial data
computed from your existing DynamoDB tables.

Usage:
    python scripts/seed_aggregates.py

Prerequisites:
    1. Create AdminAggregatesTable in AWS Console:
       - Partition Key: aggregateType (String)
       - Sort Key: aggregateId (String)
    2. AWS credentials configured
"""
import sys
from pathlib import Path
from datetime import date, timedelta
from collections import Counter, defaultdict

# Add parent directory to path for imports
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.dynamodb_service import db_service
from app.services.aggregates_service import _put_aggregate

# Table names
WALLET_TABLE = "WalletTable"
TRANSACTION_TABLE = "WalletTransactionTable"
REFERRAL_TABLE = "TierReferralTable"
LEAD_TABLE = "LeadTable"
WITHDRAWAL_TABLE = "WithdrawnTable"
USER_TABLE = "UserTable"
TIER_TABLE = "TierDetailsTable"  # Fixed: was TierTable

# Tier conversion rates
TIER_RATES = {
    'Bronze': 0.40,
    'Silver': 0.70,
    'Gold': 1.00,
    'Unknown': 0.40
}


def get_tier_name(tier_id: str, tier_cache: dict) -> str:
    """Get tier name from tier ID using cache."""
    if not tier_id or tier_id == 'unknown':
        return 'Unknown'
    
    if tier_id in tier_cache:
        return tier_cache[tier_id]
    
    # Fetch tier from DB
    try:
        tier = db_service.get_item(TIER_TABLE, {"tierId": tier_id})
        if tier:
            # Fixed: Use tierType field, not tierName
            name = tier.get('tierType', 'Unknown')
            # Normalize tier names
            name_lower = str(name).lower()
            if 'gold' in name_lower:
                name = 'Gold'
            elif 'silver' in name_lower:
                name = 'Silver'
            elif 'bronze' in name_lower:
                name = 'Bronze'
            else:
                name = 'Unknown'
            tier_cache[tier_id] = name
            return name
    except:
        pass
    
    tier_cache[tier_id] = 'Unknown'
    return 'Unknown'


def parse_date(created_time) -> str:
    """Parse created_time to date string."""
    from datetime import datetime
    
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


def seed_global_stats():
    """Seed global KPIs."""
    print("\n📊 Seeding Global Stats...")
    
    # Fetch all data
    wallets = db_service.scan_all(WALLET_TABLE, limit=None)
    referrals = db_service.scan_all(REFERRAL_TABLE, limit=None)
    leads = db_service.scan_all(LEAD_TABLE, limit=None)
    withdrawals = db_service.scan_all(WITHDRAWAL_TABLE, limit=None)
    
    # Calculate KPIs
    total_coins = sum(float(w.get('remainingAmount', 0)) for w in wallets)
    active_users = sum(1 for w in wallets if float(w.get('remainingAmount', 0)) > 0)
    
    pending = [w for w in withdrawals if w.get('status', '').lower() == 'pending']
    pending_count = len(pending)
    pending_amount = sum(float(w.get('requestedAmount', 0)) for w in pending)
    
    today = date.today().isoformat()
    today_referrals = sum(1 for r in referrals if parse_date(r.get('created_time')) == today)
    today_leads = sum(1 for l in leads if parse_date(l.get('created_time')) == today)
    
    data = {
        'totalCoins': total_coins,
        'activeUsersCount': active_users,
        'pendingWithdrawalsCount': pending_count,
        'pendingWithdrawalsAmount': pending_amount,
        'todayReferralsCount': today_referrals,
        'todayLeadsCount': today_leads
    }
    
    success = _put_aggregate("GLOBAL", "STATS", data)
    if success:
        print(f"  ✅ Global Stats: {total_coins:,.0f} coins, {active_users:,} active users")
    else:
        print(f"  ❌ Failed to write global stats")
    
    return wallets, referrals, leads, withdrawals


def seed_tier_stats(wallets: list):
    """Seed tier-wise statistics."""
    print("\n💎 Seeding Tier Stats...")
    
    # Fetch users and tiers for lookups
    users = db_service.scan_all(USER_TABLE, limit=None)
    user_map = {u.get('userId'): u for u in users}
    tier_cache = {}
    
    # Calculate tier stats
    tier_stats = {}
    for wallet in wallets:
        user_id = wallet.get('userId', '')
        coins = float(wallet.get('remainingAmount', 0))
        
        if coins <= 0:
            continue
        
        # Get user's tier
        user = user_map.get(user_id, {})
        tier_id = user.get('tierId', 'unknown')
        tier_name = get_tier_name(tier_id, tier_cache)
        
        if tier_name not in tier_stats:
            rate = TIER_RATES.get(tier_name, 0.40)
            tier_stats[tier_name] = {'coins': 0, 'rupees': 0, 'users': 0, 'rate': rate}
        
        tier_stats[tier_name]['coins'] += coins
        tier_stats[tier_name]['rupees'] += coins * tier_stats[tier_name]['rate']
        tier_stats[tier_name]['users'] += 1
    
    # Write each tier
    for tier_name in ['Gold', 'Silver', 'Bronze', 'Unknown']:
        stats = tier_stats.get(tier_name, {'coins': 0, 'rupees': 0, 'users': 0, 'rate': TIER_RATES.get(tier_name, 0.40)})
        success = _put_aggregate("TIER", tier_name, stats)
        if success:
            print(f"  ✅ {tier_name}: {stats['users']:,} users, {stats['coins']:,.0f} coins, ₹{stats['rupees']:,.0f}")
        else:
            print(f"  ❌ Failed to write {tier_name} stats")
    
    return user_map


def seed_leaderboards(wallets: list, referrals: list, leads: list, withdrawals: list, user_map: dict):
    """Seed all leaderboards."""
    print("\n🏆 Seeding Leaderboards...")
    
    transactions = db_service.scan_all(TRANSACTION_TABLE, limit=None)
    
    # Helper to get user name
    def get_user_name(user_id):
        user = user_map.get(user_id, {})
        return user.get('userName', 'Unknown')
    
    # 1. Top Coin Holders
    sorted_wallets = sorted(
        [w for w in wallets if float(w.get('remainingAmount', 0)) > 0],
        key=lambda w: float(w.get('remainingAmount', 0)),
        reverse=True
    )[:50]
    
    top_coin_items = [
        {
            'rank': i + 1,
            'userId': w.get('userId', ''),
            'userName': get_user_name(w.get('userId', '')),
            'coins': float(w.get('remainingAmount', 0))
        }
        for i, w in enumerate(sorted_wallets)
    ]
    
    success = _put_aggregate("LEADERBOARD", "TOP_COIN_HOLDERS", {'items': top_coin_items})
    print(f"  {'✅' if success else '❌'} TOP_COIN_HOLDERS: {len(top_coin_items)} items")
    
    # 2. Top Referrers
    referrer_counts = Counter(r.get('userId', '') for r in referrals if r.get('userId'))
    top_referrers = [
        {
            'rank': i + 1,
            'userId': user_id,
            'userName': get_user_name(user_id),
            'referralCount': count
        }
        for i, (user_id, count) in enumerate(referrer_counts.most_common(50))
    ]
    
    success = _put_aggregate("LEADERBOARD", "TOP_REFERRERS", {'items': top_referrers})
    print(f"  {'✅' if success else '❌'} TOP_REFERRERS: {len(top_referrers)} items")
    
    # 3. Top Lead Generators
    lead_counts = Counter(l.get('userId', '') for l in leads if l.get('userId'))
    top_leads = [
        {
            'rank': i + 1,
            'userId': user_id,
            'userName': get_user_name(user_id),
            'leadCount': count
        }
        for i, (user_id, count) in enumerate(lead_counts.most_common(50))
    ]
    
    success = _put_aggregate("LEADERBOARD", "TOP_LEAD_GENERATORS", {'items': top_leads})
    print(f"  {'✅' if success else '❌'} TOP_LEAD_GENERATORS: {len(top_leads)} items")
    
    # 4. Top Earners (sum of credits from transactions)
    user_credits = defaultdict(float)
    for txn in transactions:
        title = str(txn.get('title', '')).lower()
        amount = float(txn.get('amount', 0))
        if 'credit' in title or amount > 0:
            user_id = txn.get('userId', '')
            if user_id:
                user_credits[user_id] += abs(amount)
    
    top_earners = [
        {
            'rank': i + 1,
            'userId': user_id,
            'userName': get_user_name(user_id),
            'totalEarned': total
        }
        for i, (user_id, total) in enumerate(sorted(user_credits.items(), key=lambda x: x[1], reverse=True)[:50])
    ]
    
    success = _put_aggregate("LEADERBOARD", "TOP_EARNERS", {'items': top_earners})
    print(f"  {'✅' if success else '❌'} TOP_EARNERS: {len(top_earners)} items")
    
    # 5. Top Withdrawers
    withdrawal_counts = Counter()
    withdrawal_amounts = defaultdict(float)
    for w in withdrawals:
        user_id = w.get('userId', '')
        if user_id:
            withdrawal_counts[user_id] += 1
            withdrawal_amounts[user_id] += float(w.get('requestedAmount', 0))
    
    top_withdrawers = [
        {
            'rank': i + 1,
            'userId': user_id,
            'userName': get_user_name(user_id),
            'withdrawalCount': count,
            'totalAmount': withdrawal_amounts[user_id]
        }
        for i, (user_id, count) in enumerate(withdrawal_counts.most_common(50))
    ]
    
    success = _put_aggregate("LEADERBOARD", "TOP_WITHDRAWERS", {'items': top_withdrawers})
    print(f"  {'✅' if success else '❌'} TOP_WITHDRAWERS: {len(top_withdrawers)} items")


def seed_daily_metrics(referrals: list, leads: list):
    """Seed daily metrics for last 30 days."""
    print("\n📅 Seeding Daily Metrics...")
    
    transactions = db_service.scan_all(TRANSACTION_TABLE, limit=None)
    
    # Initialize last 30 days
    daily_data = {}
    for i in range(30):
        day = (date.today() - timedelta(days=i)).isoformat()
        daily_data[day] = {'referrals': 0, 'leads': 0, 'coinCredits': 0, 'coinDebits': 0}
    
    # Count referrals per day
    for r in referrals:
        day = parse_date(r.get('created_time'))
        if day in daily_data:
            daily_data[day]['referrals'] += 1
    
    # Count leads per day
    for l in leads:
        day = parse_date(l.get('created_time'))
        if day in daily_data:
            daily_data[day]['leads'] += 1
    
    # Sum credits/debits per day
    for txn in transactions:
        day = parse_date(txn.get('created_time'))
        if day in daily_data:
            amount = float(txn.get('amount', 0))
            title = str(txn.get('title', '')).lower()
            
            if 'credit' in title or amount > 0:
                daily_data[day]['coinCredits'] += abs(amount)
            elif 'debit' in title or amount < 0:
                daily_data[day]['coinDebits'] += abs(amount)
    
    # Write each day
    success_count = 0
    for day, data in sorted(daily_data.items()):
        if _put_aggregate("DAILY", day, data):
            success_count += 1
    
    print(f"  ✅ Wrote {success_count}/30 daily records")


def main():
    """Main seed function."""
    print("=" * 60)
    print("🚀 SEEDING ADMIN AGGREGATES TABLE")
    print("=" * 60)
    
    try:
        # Seed global stats (returns fetched data for reuse)
        wallets, referrals, leads, withdrawals = seed_global_stats()
        
        # Seed tier stats (returns user map for reuse)
        user_map = seed_tier_stats(wallets)
        
        # Seed leaderboards
        seed_leaderboards(wallets, referrals, leads, withdrawals, user_map)
        
        # Seed daily metrics
        seed_daily_metrics(referrals, leads)
        
        print("\n" + "=" * 60)
        print("✅ SEEDING COMPLETE!")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Verify data in AWS Console (AdminAggregatesTable)")
        print("2. Set USE_AGGREGATES = True in aggregates_service.py")
        print("3. Restart Streamlit app and test dashboard")
        
    except Exception as e:
        print(f"\n❌ Error during seeding: {e}")
        raise


if __name__ == "__main__":
    main()
