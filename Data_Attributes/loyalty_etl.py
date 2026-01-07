"""
Loyalty ETL Pipeline - Extract, Transform, Load to Redshift

This script extracts data from DynamoDB loyalty tables, transforms it through
a 3-layer S3 pipeline (raw → processed → unified), and loads to Redshift.

S3 Bucket: etl-bucket-27-10-2025
Target Schema: loyalty

Usage:
    python loyalty_etl.py --extract      # DynamoDB → S3 Raw (JSON)
    python loyalty_etl.py --transform    # S3 Raw → S3 Processed (CSV)
    python loyalty_etl.py --unify        # S3 Processed → S3 Unified (CSV)
    python loyalty_etl.py --load         # S3 Unified → Redshift
    python loyalty_etl.py --full         # Run all steps
"""

import boto3
import json
import csv
import io
import os
import re
from datetime import datetime, date
from decimal import Decimal
from typing import List, Dict, Any, Optional
from collections import defaultdict
from pathlib import Path

# Load environment variables from .env file
from dotenv import load_dotenv
env_path = Path(__file__).parent.parent / '.env'
load_dotenv(env_path)

# Configuration (all from environment variables)
S3_BUCKET = os.environ.get("S3_ETL_BUCKET", "etl-bucket-05-01-2026")
AWS_REGION = os.environ.get("AWS_DEFAULT_REGION", "ap-south-1")

# DynamoDB tables to extract for loyalty schema
LOYALTY_TABLES = [
    "UserTable",
    "WalletTable", 
    "WalletTransactionTable",
    "TierReferralTable",
    "TierDetailsTable",
    "LeadTable",
    "WithdrawnTable"
]

# Redshift configuration (ALL from environment variables - no defaults for sensitive data)
REDSHIFT_HOST = os.environ.get("REDSHIFT_HOST")
REDSHIFT_PORT = int(os.environ.get("REDSHIFT_PORT", "5439"))
REDSHIFT_DATABASE = os.environ.get("REDSHIFT_DATABASE")
REDSHIFT_USER = os.environ.get("REDSHIFT_USER")
REDSHIFT_PASSWORD = os.environ.get("REDSHIFT_PASSWORD")
REDSHIFT_IAM_ROLE = os.environ.get("REDSHIFT_IAM_ROLE")


class DecimalEncoder(json.JSONEncoder):
    """Handle Decimal types from DynamoDB."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def convert_decimals(obj):
    """Recursively convert Decimal objects to int/float."""
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    return obj


def normalize_phone(phone: str) -> str:
    """
    Normalize phone number to 10 digits.
    Handles: +919704612333, 919704612333, 9704612333
    """
    if not phone:
        return ""
    # Remove non-digit characters
    digits = re.sub(r'\D', '', str(phone))
    # Remove country code if present
    if len(digits) == 12 and digits.startswith('91'):
        return digits[2:]
    if len(digits) == 13 and digits.startswith('91'):
        return digits[3:]
    if len(digits) == 10:
        return digits
    return digits[-10:] if len(digits) > 10 else digits


def parse_timestamp(ts: Any) -> Optional[str]:
    """
    Parse timestamp from various formats to ISO string.
    Handles: milliseconds, seconds, ISO strings
    """
    if ts is None:
        return None
    
    if isinstance(ts, (int, float)):
        try:
            # Assume milliseconds if > 10 billion
            if ts > 10000000000:
                ts = ts / 1000
            return datetime.fromtimestamp(ts).isoformat()
        except:
            return None
    
    if isinstance(ts, str):
        return ts
    
    return None


class LoyaltyETL:
    """ETL Pipeline for Loyalty Data Mart."""
    
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
        self.s3 = boto3.client('s3', region_name=AWS_REGION)
        self.today = date.today()
        self.run_timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    def get_s3_path(self, layer: str, table_name: str) -> str:
        """Generate S3 path following the data lake structure."""
        return (
            f"{layer}/dynamodb/{table_name}/"
            f"year={self.today.year}/month={self.today.month:02d}/day={self.today.day:02d}/"
        )
    
    def get_unified_path(self, table_name: str) -> str:
        """Generate S3 path for unified layer (loyalty schema)."""
        return (
            f"processed/unified/loyalty/{table_name}/"
            f"year={self.today.year}/month={self.today.month:02d}/day={self.today.day:02d}/"
        )
    
    # =========================================================================
    # LAYER 1: EXTRACT - DynamoDB → S3 Raw (JSON)
    # =========================================================================
    
    def scan_table_with_pagination(self, table_name: str) -> List[Dict]:
        """
        Scan DynamoDB table with full pagination.
        Returns all items regardless of table size.
        """
        table = self.dynamodb.Table(table_name)
        items = []
        scan_kwargs = {}
        page_count = 0
        
        print(f"  Scanning {table_name}...")
        
        while True:
            response = table.scan(**scan_kwargs)
            batch = response.get('Items', [])
            items.extend(batch)
            page_count += 1
            
            print(f"    Page {page_count}: {len(batch)} items (total: {len(items)})")
            
            # Check for more pages
            if 'LastEvaluatedKey' not in response:
                break
            
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        
        print(f"  [OK] {table_name}: {len(items)} items from {page_count} pages")
        return convert_decimals(items)
    
    def extract_to_raw(self) -> Dict[str, int]:
        """
        Extract all loyalty tables from DynamoDB to S3 raw layer.
        Returns: {table_name: row_count}
        """
        print("\n" + "=" * 60)
        print("LAYER 1: EXTRACT (DynamoDB → S3 Raw JSON)")
        print("=" * 60)
        
        results = {}
        
        for table_name in LOYALTY_TABLES:
            try:
                # Scan with full pagination
                items = self.scan_table_with_pagination(table_name)
                
                if not items:
                    print(f"  [WARN] {table_name}: No items found")
                    results[table_name] = 0
                    continue
                
                # Upload to S3 as JSON
                s3_path = self.get_s3_path("raw", table_name) + "data.json"
                json_data = json.dumps(items, cls=DecimalEncoder, indent=2)
                
                self.s3.put_object(
                    Bucket=S3_BUCKET,
                    Key=s3_path,
                    Body=json_data.encode('utf-8'),
                    ContentType='application/json'
                )
                
                print(f"  → s3://{S3_BUCKET}/{s3_path}")
                results[table_name] = len(items)
                
            except Exception as e:
                print(f"  [ERROR] {table_name}: {str(e)}")
                results[table_name] = -1
        
        return results
    
    # =========================================================================
    # LAYER 2: TRANSFORM - S3 Raw → S3 Processed (CSV)
    # =========================================================================
    
    def read_raw_json(self, table_name: str) -> List[Dict]:
        """Read raw JSON from S3."""
        s3_path = self.get_s3_path("raw", table_name) + "data.json"
        
        try:
            response = self.s3.get_object(Bucket=S3_BUCKET, Key=s3_path)
            return json.loads(response['Body'].read().decode('utf-8'))
        except Exception as e:
            print(f"  [ERROR] Cannot read {s3_path}: {e}")
            return []
    
    def transform_users(self, items: List[Dict]) -> List[Dict]:
        """Transform UserTable items."""
        transformed = []
        for item in items:
            transformed.append({
                'user_id': item.get('userId', ''),
                'user_name': item.get('userName', ''),
                'phone_number': item.get('phoneNumber', ''),
                'phone_normalized': normalize_phone(item.get('phoneNumber', '')),
                'email': item.get('emailId', ''),
                'tier_id': item.get('tierId', ''),
                'referral_code': item.get('referralCode', ''),
                'created_at': parse_timestamp(item.get('created_time'))
            })
        return transformed
    
    def transform_wallets(self, items: List[Dict]) -> List[Dict]:
        """Transform WalletTable items."""
        transformed = []
        for item in items:
            transformed.append({
                'wallet_id': item.get('walletId', ''),
                'user_id': item.get('userId', ''),
                'remaining_amount': float(item.get('remainingAmount', 0)),
                'total_amount': float(item.get('totalAmount', 0)),
                'used_amount': float(item.get('usedAmount', 0))
            })
        return transformed
    
    def transform_transactions(self, items: List[Dict]) -> List[Dict]:
        """Transform WalletTransactionTable items."""
        transformed = []
        for item in items:
            amount = float(item.get('amount', 0))
            transformed.append({
                'transaction_id': item.get('transactionId', ''),
                'user_id': item.get('userId', ''),
                'transaction_type': 'credit' if amount >= 0 else 'debit',
                'title': item.get('title', ''),
                'amount': amount,
                'reason': item.get('reason', ''),
                'status': item.get('status', ''),
                'created_at': parse_timestamp(item.get('created_time'))
            })
        return transformed
    
    def transform_referrals(self, items: List[Dict]) -> List[Dict]:
        """Transform TierReferralTable items."""
        transformed = []
        for item in items:
            transformed.append({
                'referral_id': item.get('tierReferralId', ''),
                'referrer_user_id': item.get('userId', ''),
                'referred_phone': item.get('sentTo', ''),
                'referred_phone_normalized': normalize_phone(item.get('sentTo', '')),
                'referral_code': item.get('appliedCode', ''),
                'status': 'applied' if item.get('appliedCode') else 'pending',
                'created_at': parse_timestamp(item.get('created_time'))
            })
        return transformed
    
    def transform_tiers(self, items: List[Dict]) -> List[Dict]:
        """Transform TierDetailsTable items."""
        # Map tierType (GOLD/SILVER/BRONZE) to display name and redemption rates
        tier_config = {
            'GOLD': {'name': 'Gold', 'rate': 1.0},
            'SILVER': {'name': 'Silver', 'rate': 0.7},
            'BRONZE': {'name': 'Bronze', 'rate': 0.4}
        }
        transformed = []
        for item in items:
            tier_type = item.get('tierType', 'BRONZE').upper()
            config = tier_config.get(tier_type, {'name': 'Unknown', 'rate': 0.4})
            transformed.append({
                'tier_id': item.get('tierId', ''),
                'tier_name': config['name'],
                'redemption_rate': config['rate']
            })
        return transformed
    
    def transform_leads(self, items: List[Dict]) -> List[Dict]:
        """Transform LeadTable items."""
        transformed = []
        for item in items:
            transformed.append({
                'lead_id': item.get('leadId', ''),
                'generator_user_id': item.get('userId', ''),
                'lead_name': item.get('leadName', ''),
                'lead_phone': item.get('leadPhoneNumber', ''),
                'occasion_name': item.get('occasionName', ''),
                'lead_stage': item.get('leadStage', ''),
                'estimated_value': float(item.get('estimatedValue', 0)),
                'created_at': parse_timestamp(item.get('created_time'))
            })
        return transformed
    
    def transform_withdrawals(self, items: List[Dict]) -> List[Dict]:
        """Transform WithdrawnTable items."""
        transformed = []
        for item in items:
            transformed.append({
                'withdrawal_id': item.get('requestedId', ''),
                'user_id': item.get('userId', ''),
                'requested_amount': float(item.get('requestedAmount', 0)),
                'approved_amount': float(item.get('approvedAmount', 0)) if item.get('approvedAmount') else None,
                'status': item.get('status', ''),
                'bank_id': item.get('bankId', ''),
                'upi_id': item.get('upiId', ''),
                'created_at': parse_timestamp(item.get('created_time')),
                'processed_at': parse_timestamp(item.get('updated_time'))
            })
        return transformed
    
    def write_csv_to_s3(self, data: List[Dict], s3_path: str) -> int:
        """Write list of dicts to S3 as CSV."""
        if not data:
            return 0
        
        # Create CSV in memory
        output = io.StringIO()
        writer = csv.DictWriter(output, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
        
        # Upload to S3
        self.s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_path,
            Body=output.getvalue().encode('utf-8'),
            ContentType='text/csv'
        )
        
        return len(data)
    
    def transform_to_processed(self) -> Dict[str, int]:
        """
        Transform raw JSON to processed CSV with cleaning.
        Returns: {table_name: row_count}
        """
        print("\n" + "=" * 60)
        print("LAYER 2: TRANSFORM (S3 Raw JSON → S3 Processed CSV)")
        print("=" * 60)
        
        transforms = {
            'UserTable': self.transform_users,
            'WalletTable': self.transform_wallets,
            'WalletTransactionTable': self.transform_transactions,
            'TierReferralTable': self.transform_referrals,
            'TierDetailsTable': self.transform_tiers,
            'LeadTable': self.transform_leads,
            'WithdrawnTable': self.transform_withdrawals
        }
        
        results = {}
        
        for table_name, transform_fn in transforms.items():
            try:
                # Read raw JSON
                raw_data = self.read_raw_json(table_name)
                if not raw_data:
                    results[table_name] = 0
                    continue
                
                # Transform
                transformed = transform_fn(raw_data)
                
                # Write to processed layer
                s3_path = self.get_s3_path("processed", table_name) + "data.csv"
                count = self.write_csv_to_s3(transformed, s3_path)
                
                print(f"  [OK] {table_name}: {count} rows -> s3://{S3_BUCKET}/{s3_path}")
                results[table_name] = count
                
            except Exception as e:
                print(f"  [ERROR] {table_name}: {str(e)}")
                results[table_name] = -1
        
        return results
    
    # =========================================================================
    # LAYER 3: UNIFY - S3 Processed → S3 Unified (for Redshift)
    # =========================================================================
    
    def read_processed_csv(self, table_name: str) -> List[Dict]:
        """Read processed CSV from S3."""
        s3_path = self.get_s3_path("processed", table_name) + "data.csv"
        
        try:
            response = self.s3.get_object(Bucket=S3_BUCKET, Key=s3_path)
            content = response['Body'].read().decode('utf-8')
            reader = csv.DictReader(io.StringIO(content))
            return list(reader)
        except Exception as e:
            print(f"  [ERROR] Cannot read {s3_path}: {e}")
            return []
    
    def unify_for_redshift(self) -> Dict[str, int]:
        """
        Create unified CSVs ready for Redshift COPY command.
        Joins User + Wallet, enriches with names, etc.
        """
        print("\n" + "=" * 60)
        print("LAYER 3: UNIFY (S3 Processed → S3 Unified for Redshift)")
        print("=" * 60)
        
        results = {}
        
        # Load lookup data
        users = {u['user_id']: u for u in self.read_processed_csv('UserTable')}
        wallets = {w['user_id']: w for w in self.read_processed_csv('WalletTable')}
        tiers = {t['tier_id']: t for t in self.read_processed_csv('TierDetailsTable')}
        
        print(f"  Loaded: {len(users)} users, {len(wallets)} wallets, {len(tiers)} tiers")
        
        # --- dim_tier ---
        tier_data = list(tiers.values())
        s3_path = self.get_unified_path("dim_tier") + "data.csv"
        results['dim_tier'] = self.write_csv_to_s3(tier_data, s3_path)
        print(f"  [OK] dim_tier: {results['dim_tier']} rows")
        
        # --- dim_loyalty_users (User + Wallet joined) ---
        dim_users = []
        for user_id, user in users.items():
            wallet = wallets.get(user_id, {})
            tier = tiers.get(user.get('tier_id'), {})
            
            dim_users.append({
                'user_id': user_id,
                'user_name': user.get('user_name', ''),
                'phone_number': user.get('phone_number', ''),
                'phone_normalized': user.get('phone_normalized', ''),
                'email': user.get('email', ''),
                'tier_id': user.get('tier_id', ''),
                'tier_name': tier.get('tier_name', 'Unknown'),
                'referral_code': user.get('referral_code', ''),
                'remaining_coins': wallet.get('remaining_amount', 0),
                'total_earned': wallet.get('total_amount', 0),
                'total_used': wallet.get('used_amount', 0),
                'signup_date': user.get('created_at', '')
            })
        
        s3_path = self.get_unified_path("dim_loyalty_users") + "data.csv"
        results['dim_loyalty_users'] = self.write_csv_to_s3(dim_users, s3_path)
        print(f"  [OK] dim_loyalty_users: {results['dim_loyalty_users']} rows (User + Wallet joined)")
        
        # --- fact_wallet_transactions ---
        transactions = self.read_processed_csv('WalletTransactionTable')
        s3_path = self.get_unified_path("fact_wallet_transactions") + "data.csv"
        results['fact_wallet_transactions'] = self.write_csv_to_s3(transactions, s3_path)
        print(f"  [OK] fact_wallet_transactions: {results['fact_wallet_transactions']} rows")
        
        # --- fact_referrals (enriched with names) ---
        referrals = self.read_processed_csv('TierReferralTable')
        enriched_referrals = []
        for ref in referrals:
            referrer = users.get(ref.get('referrer_user_id'), {})
            # Check if referred phone signed up
            referred_user = None
            for u in users.values():
                if u.get('phone_normalized') == ref.get('referred_phone_normalized'):
                    referred_user = u
                    break
            
            enriched_referrals.append({
                **ref,
                'referrer_name': referrer.get('user_name', ''),
                'referred_name': referred_user.get('user_name', '') if referred_user else '',
                'referred_user_id': referred_user.get('user_id', '') if referred_user else ''
            })
        
        s3_path = self.get_unified_path("fact_referrals") + "data.csv"
        results['fact_referrals'] = self.write_csv_to_s3(enriched_referrals, s3_path)
        print(f"  [OK] fact_referrals: {results['fact_referrals']} rows (enriched with names)")
        
        # --- fact_leads (enriched with generator name) ---
        leads = self.read_processed_csv('LeadTable')
        enriched_leads = []
        for lead in leads:
            generator = users.get(lead.get('generator_user_id'), {})
            enriched_leads.append({
                **lead,
                'generator_name': generator.get('user_name', '')
            })
        
        s3_path = self.get_unified_path("fact_leads") + "data.csv"
        results['fact_leads'] = self.write_csv_to_s3(enriched_leads, s3_path)
        print(f"  [OK] fact_leads: {results['fact_leads']} rows (enriched)")
        
        # --- fact_withdrawals (enriched with user name) ---
        withdrawals = self.read_processed_csv('WithdrawnTable')
        enriched_withdrawals = []
        for w in withdrawals:
            user = users.get(w.get('user_id'), {})
            enriched_withdrawals.append({
                **w,
                'user_name': user.get('user_name', '')
            })
        
        s3_path = self.get_unified_path("fact_withdrawals") + "data.csv"
        results['fact_withdrawals'] = self.write_csv_to_s3(enriched_withdrawals, s3_path)
        print(f"  [OK] fact_withdrawals: {results['fact_withdrawals']} rows (enriched)")
        
        return results
    
    # =========================================================================
    # LAYER 4: LOAD - S3 Unified → Redshift
    # =========================================================================
    
    def generate_copy_commands(self) -> str:
        """
        Generate Redshift COPY commands for all unified tables.
        User should run these in Redshift Query Editor.
        """
        print("\n" + "=" * 60)
        print("LAYER 4: LOAD (S3 Unified -> Redshift)")
        print("=" * 60)
        
        tables = [
            'dim_tier',
            'dim_loyalty_users',
            'fact_wallet_transactions',
            'fact_referrals',
            'fact_leads',
            'fact_withdrawals'
        ]
        
        commands = []
        commands.append("-- Run these commands in Redshift Query Editor")
        commands.append("-- First, create the schema if not exists:")
        commands.append("CREATE SCHEMA IF NOT EXISTS loyalty;\n")
        
        for table in tables:
            s3_path = f"s3://{S3_BUCKET}/{self.get_unified_path(table)}data.csv"
            
            cmd = f"""
-- Load {table}
TRUNCATE TABLE loyalty.{table};
COPY loyalty.{table}
FROM '{s3_path}'
IAM_ROLE '{REDSHIFT_IAM_ROLE}'
CSV
IGNOREHEADER 1
DATEFORMAT 'auto'
TIMEFORMAT 'auto'
BLANKSASNULL
EMPTYASNULL;
"""
            commands.append(cmd)
        
        sql = "\n".join(commands)
        print(sql)
        
        # Save to S3 for reference
        s3_path = f"metadata/runs/loyalty/year={self.today.year}/month={self.today.month:02d}/day={self.today.day:02d}/copy_commands.sql"
        self.s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_path,
            Body=sql.encode('utf-8'),
            ContentType='text/plain'
        )
        print(f"\n  -> Saved to s3://{S3_BUCKET}/{s3_path}")
        
        return sql
    
    def write_execution_log(self, extract_results: Dict, transform_results: Dict, unify_results: Dict):
        """Write execution metadata to S3."""
        log = {
            'run_timestamp': self.run_timestamp,
            'date': self.today.isoformat(),
            'extract': extract_results,
            'transform': transform_results,
            'unify': unify_results,
            'status': 'success'
        }
        
        s3_path = f"metadata/runs/loyalty/year={self.today.year}/month={self.today.month:02d}/day={self.today.day:02d}/execution_log.json"
        
        self.s3.put_object(
            Bucket=S3_BUCKET,
            Key=s3_path,
            Body=json.dumps(log, indent=2).encode('utf-8'),
            ContentType='application/json'
        )
        
        print(f"\n[LOG] Execution log: s3://{S3_BUCKET}/{s3_path}")
    
    def run_full_pipeline(self):
        """Run the complete ETL pipeline."""
        print("\n" + "=" * 60)
        print("[ROCKET] LOYALTY ETL PIPELINE - FULL RUN")
        print(f"   Date: {self.today.isoformat()}")
        print(f"   Bucket: s3://{S3_BUCKET}")
        print("=" * 60)
        
        # Layer 1: Extract
        extract_results = self.extract_to_raw()
        
        # Layer 2: Transform
        transform_results = self.transform_to_processed()
        
        # Layer 3: Unify
        unify_results = self.unify_for_redshift()
        
        # Layer 4: Generate COPY commands
        self.generate_copy_commands()
        
        # Write execution log
        self.write_execution_log(extract_results, transform_results, unify_results)
        
        print("\n" + "=" * 60)
        print("[SUCCESS] ETL PIPELINE COMPLETE")
        print("=" * 60)
        print("\nNext steps:")
        print("1. Run the COPY commands in Redshift Query Editor")
        print("2. Or use the SQL file from S3 metadata folder")


if __name__ == "__main__":
    import sys
    
    etl = LoyaltyETL()
    
    if len(sys.argv) < 2 or '--full' in sys.argv:
        etl.run_full_pipeline()
    elif '--extract' in sys.argv:
        etl.extract_to_raw()
    elif '--transform' in sys.argv:
        etl.transform_to_processed()
    elif '--unify' in sys.argv:
        etl.unify_for_redshift()
    elif '--load' in sys.argv:
        etl.generate_copy_commands()
    else:
        print("Usage: python loyalty_etl.py [--extract|--transform|--unify|--load|--full]")
