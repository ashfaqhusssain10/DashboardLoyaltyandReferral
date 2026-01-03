"""
Download WalletTransactionTable data to CSV with pagination support.

Usage: python download_transaction_data.py
"""
import sys
import csv
import io
from pathlib import Path
from datetime import datetime
from collections import defaultdict

# Fix Windows encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.dynamodb_service import db_service


def download_transaction_table_to_csv(output_path: str = None):
    """
    Download all data from WalletTransactionTable to CSV with pagination.
    Also shows Top Earners calculation for verification.
    
    Args:
        output_path: Path to save CSV. Defaults to Data_Attributes/transaction_data_YYYYMMDD.csv
    """
    
    print("=" * 70)
    print("WALLET TRANSACTION TABLE DOWNLOAD")
    print("=" * 70)
    
    # Default output path
    if output_path is None:
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(__file__).parent / f"transaction_data_{date_str}.csv"
    else:
        output_path = Path(output_path)
    
    # Fetch all data with pagination
    print("\n[1/4] Fetching data from WalletTransactionTable...")
    print("      This may take a while for large tables...")
    
    table_name = "WalletTransactionTable"
    items = []
    page_count = 0
    
    # Manual pagination for visibility
    table = db_service.get_table(table_name)
    scan_kwargs = {}
    
    while True:
        response = table.scan(**scan_kwargs)
        batch = response.get('Items', [])
        items.extend(batch)
        page_count += 1
        
        print(f"      Page {page_count}: Fetched {len(batch)} items (Total: {len(items)})")
        
        # Check if there are more pages
        if 'LastEvaluatedKey' not in response:
            break
        
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
    
    print(f"\n[2/4] Total transactions fetched: {len(items)}")
    
    if not items:
        print("      No data found in WalletTransactionTable!")
        return
    
    # Collect all unique keys across all items
    all_keys = set()
    for item in items:
        all_keys.update(item.keys())
    
    # Define column order (important fields first)
    priority_cols = ['transactionId', 'userId', 'title', 'amount', 'reason', 'status', 'created_time']
    other_cols = sorted([k for k in all_keys if k not in priority_cols])
    columns = priority_cols + other_cols
    
    # Write to CSV
    print(f"\n[3/4] Writing to CSV: {output_path}")
    
    with open(output_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=columns, extrasaction='ignore')
        writer.writeheader()
        
        for item in items:
            # Convert Decimal to float/int for CSV
            row = {}
            for key, value in item.items():
                if hasattr(value, '__float__'):
                    row[key] = float(value)
                else:
                    row[key] = value
            writer.writerow(row)
    
    print(f"\n{'=' * 70}")
    print(f"SUCCESS! Downloaded {len(items)} transactions to:")
    print(f"  {output_path}")
    print(f"{'=' * 70}")
    
    # ===== ANALYSIS FOR TOP EARNERS =====
    print("\n[4/4] TRANSACTION ANALYSIS FOR TOP EARNERS")
    print("=" * 70)
    
    # Analyze transaction types (titles)
    title_counts = defaultdict(int)
    title_amounts = defaultdict(float)
    for item in items:
        title = str(item.get('title', 'Unknown'))
        amount = float(item.get('amount', 0) or 0)
        title_counts[title] += 1
        title_amounts[title] += amount
    
    print("\n[A] Transaction Types (Title Analysis):")
    print("-" * 70)
    print(f"{'Title':<40} {'Count':>10} {'Total Amt':>15}")
    print("-" * 70)
    for title in sorted(title_counts.keys()):
        print(f"{title:<40} {title_counts[title]:>10} {title_amounts[title]:>15,.0f}")
    
    # Calculate Top Earners (current logic)
    print("\n[B] TOP EARNERS (Current Logic: 'credit' in title OR amount > 0):")
    print("-" * 70)
    
    user_credits = defaultdict(float)
    for txn in items:
        title = str(txn.get('title', '')).lower()
        amount = float(txn.get('amount', 0) or 0)
        if 'credit' in title or amount > 0:
            user_id = txn.get('userId', '')
            if user_id:
                user_credits[user_id] += abs(amount)
    
    # Get top 10
    top_earners = sorted(user_credits.items(), key=lambda x: x[1], reverse=True)[:10]
    
    print(f"{'Rank':<6} {'User ID':<40} {'Total Earned':>15}")
    print("-" * 70)
    for i, (user_id, total) in enumerate(top_earners, 1):
        print(f"{i:<6} {user_id:<40} {total:>15,.0f}")
    
    # Summary
    print("\n[C] Summary Statistics:")
    print("-" * 70)
    total_credits = sum(float(item.get('amount', 0) or 0) for item in items if float(item.get('amount', 0) or 0) > 0)
    total_debits = sum(abs(float(item.get('amount', 0) or 0)) for item in items if float(item.get('amount', 0) or 0) < 0)
    unique_users = len(set(item.get('userId', '') for item in items))
    
    print(f"  - Total Transactions: {len(items):,}")
    print(f"  - Unique Users: {unique_users:,}")
    print(f"  - Total Credits: {total_credits:,.0f}")
    print(f"  - Total Debits: {total_debits:,.0f}")
    print(f"  - Unique Title Types: {len(title_counts)}")
    
    return str(output_path)


if __name__ == "__main__":
    # Check for custom output path
    output = None
    if len(sys.argv) > 1:
        output = sys.argv[1]
    
    download_transaction_table_to_csv(output)
