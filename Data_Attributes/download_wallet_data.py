"""
Download WalletTable data to CSV with pagination support.

Usage: python download_wallet_data.py
"""
import sys
import csv
import io
from pathlib import Path
from datetime import datetime

# Fix Windows encoding
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from app.services.dynamodb_service import db_service


def download_wallet_table_to_csv(output_path: str = None):
    """
    Download all data from WalletTable to CSV with pagination.
    
    Args:
        output_path: Path to save CSV. Defaults to Data_Attributes/wallet_data_YYYYMMDD.csv
    """
    
    print("=" * 60)
    print("WALLET TABLE DOWNLOAD")
    print("=" * 60)
    
    # Default output path
    if output_path is None:
        date_str = datetime.now().strftime("%Y%m%d_%H%M%S")
        output_path = Path(__file__).parent / f"wallet_data_{date_str}.csv"
    else:
        output_path = Path(output_path)
    
    # Fetch all data with pagination (handled internally by scan_all)
    print("\n[1/3] Fetching data from WalletTable...")
    print("      This may take a while for large tables...")
    
    table_name = "WalletTable"
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
    
    print(f"\n[2/3] Total items fetched: {len(items)}")
    
    if not items:
        print("      No data found in WalletTable!")
        return
    
    # Collect all unique keys across all items
    all_keys = set()
    for item in items:
        all_keys.update(item.keys())
    
    # Define column order (important fields first)
    priority_cols = ['walletId', 'userId', 'remainingAmount', 'totalAmount', 'usedAmount', 'created_time', 'updated_time']
    other_cols = sorted([k for k in all_keys if k not in priority_cols])
    columns = priority_cols + other_cols
    
    # Write to CSV
    print(f"\n[3/3] Writing to CSV: {output_path}")
    
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
    
    print(f"\n{'=' * 60}")
    print(f"SUCCESS! Downloaded {len(items)} records to:")
    print(f"  {output_path}")
    print(f"{'=' * 60}")
    
    # Print summary stats
    print("\nSummary Statistics:")
    total_remaining = sum(float(item.get('remainingAmount', 0) or 0) for item in items)
    total_used = sum(float(item.get('usedAmount', 0) or 0) for item in items)
    users_with_balance = sum(1 for item in items if float(item.get('remainingAmount', 0) or 0) > 0)
    
    print(f"  - Total Remaining Amount: {total_remaining:,.0f}")
    print(f"  - Total Used Amount: {total_used:,.0f}")
    print(f"  - Users with Balance > 0: {users_with_balance:,}")
    
    return str(output_path)


if __name__ == "__main__":
    # Check for custom output path
    output = None
    if len(sys.argv) > 1:
        output = sys.argv[1]
    
    download_wallet_table_to_csv(output)
