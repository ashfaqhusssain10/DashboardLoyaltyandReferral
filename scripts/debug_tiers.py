"""
Tier Debugging Script

Investigates why tier mapping is showing all users as "Unknown".
Checks:
1. UserTable tierId field presence
2. TierTable structure and content
3. Mapping between UserTable.tierId and TierTable.tierId
"""
import sys
import io
from pathlib import Path
from collections import Counter

# Fix Windows encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.dynamodb_service import db_service

USER_TABLE = "UserTable"
TIER_TABLE = "TierDetailsTable"  # Fixed: was TierTable
WALLET_TABLE = "WalletTable"


def main():
    print("=" * 70)
    print("[DEBUG] TIER DEBUGGING SCRIPT")
    print("=" * 70)
    
    # 1. Check TierTable contents
    print("\n📋 Step 1: Checking TierTable...")
    tiers = db_service.scan_all(TIER_TABLE, limit=100)
    print(f"   Found {len(tiers)} tiers in TierTable")
    
    if tiers:
        print("\n   Tier Records:")
        for i, tier in enumerate(tiers, 1):
            tier_id = tier.get('tierId', 'N/A')
            tier_name = tier.get('tierName', 'N/A')
            print(f"   {i}. tierId='{tier_id}' → tierName='{tier_name}'")
            # Show all attributes for first tier
            if i == 1:
                print(f"      All attributes: {list(tier.keys())}")
    else:
        print("   ⚠️ TierTable is EMPTY!")
    
    # 2. Check UserTable tierId field
    print("\n📋 Step 2: Checking UserTable tierId field...")
    # Get sample of users
    users_sample = db_service.scan_all(USER_TABLE, limit=100)
    print(f"   Sample size: {len(users_sample)} users")
    
    # Count tierId presence
    has_tier = sum(1 for u in users_sample if u.get('tierId'))
    no_tier = sum(1 for u in users_sample if not u.get('tierId'))
    
    print(f"\n   Users WITH tierId: {has_tier}")
    print(f"   Users WITHOUT tierId: {no_tier}")
    
    # Show sample tierId values
    tier_ids_found = Counter(u.get('tierId', 'MISSING') for u in users_sample)
    print(f"\n   tierId distribution (sample):")
    for tier_id, count in tier_ids_found.most_common(10):
        print(f"   - '{tier_id}': {count} users")
    
    # 3. Show a few user records to understand structure
    print("\n📋 Step 3: Sample User Records...")
    for i, user in enumerate(users_sample[:3], 1):
        print(f"\n   User {i}:")
        print(f"      userId: {user.get('userId', 'N/A')[:30]}...")
        print(f"      userName: {user.get('userName', 'N/A')}")
        print(f"      tierId: {user.get('tierId', 'MISSING')}")
        print(f"      All attributes: {list(user.keys())}")
    
    # 4. Cross-check: Do any user tierIds match TierTable?
    print("\n📋 Step 4: Cross-checking tierId mapping...")
    tier_ids_in_table = set(t.get('tierId') for t in tiers)
    user_tier_ids = set(u.get('tierId') for u in users_sample if u.get('tierId'))
    
    matching = user_tier_ids.intersection(tier_ids_in_table)
    not_matching = user_tier_ids - tier_ids_in_table
    
    print(f"   TierTable tierId values: {tier_ids_in_table}")
    print(f"   UserTable tierId values (sample): {user_tier_ids}")
    print(f"\n   ✅ Matching tierIds: {matching}")
    print(f"   ❌ UserTable tierIds NOT in TierTable: {not_matching}")
    
    # 5. Check users with balance specifically
    print("\n📋 Step 5: Checking users WITH balance...")
    wallets = db_service.scan_all(WALLET_TABLE, limit=500)
    wallets_with_balance = [w for w in wallets if float(w.get('remainingAmount', 0)) > 0]
    print(f"   Found {len(wallets_with_balance)} wallets with balance")
    
    # Get these specific users
    users_with_balance_tiers = []
    for w in wallets_with_balance[:50]:  # Check first 50
        user_id = w.get('userId')
        user = db_service.get_item(USER_TABLE, {"userId": user_id})
        if user:
            users_with_balance_tiers.append(user.get('tierId', 'MISSING'))
    
    tier_dist = Counter(users_with_balance_tiers)
    print(f"\n   tierId distribution for users WITH balance:")
    for tier_id, count in tier_dist.most_common(10):
        print(f"   - '{tier_id}': {count} users")
    
    print("\n" + "=" * 70)
    print("🔍 DIAGNOSIS COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    main()
