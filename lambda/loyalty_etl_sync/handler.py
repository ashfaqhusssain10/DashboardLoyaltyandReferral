"""
AWS Lambda Handler for Loyalty ETL Sync

Triggered by EventBridge Scheduler to run daily ETL pipeline.

NOTE: Lambda has 15-minute timeout. For large datasets, consider:
- AWS Glue ETL job
- ECS Fargate task
- Step Functions with parallel processing
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

# Configuration from environment
S3_BUCKET = os.environ.get("S3_ETL_BUCKET", "etl-bucket-05-01-2026")
AWS_REGION = os.environ.get("AWS_REGION", "ap-south-1")
REDSHIFT_IAM_ROLE = os.environ.get("REDSHIFT_IAM_ROLE")

# Redshift Data API Configuration
REDSHIFT_WORKGROUP = os.environ.get("REDSHIFT_WORKGROUP")  # For serverless
REDSHIFT_CLUSTER_ID = os.environ.get("REDSHIFT_CLUSTER_ID", "data-pipeline-cluster")  # For provisioned
REDSHIFT_DATABASE = os.environ.get("REDSHIFT_DATABASE", "datawarehouse")
REDSHIFT_USER = os.environ.get("REDSHIFT_USER", "admin")
ENABLE_REDSHIFT_LOAD = os.environ.get("ENABLE_REDSHIFT_LOAD", "true").lower() == "true"

# DynamoDB tables
LOYALTY_TABLES = [
    "UserTable",
    "WalletTable", 
    "WalletTransactionTable",
    "TierReferralTable",
    "TierDetailsTable",
    "LeadTable",
    "WithdrawnTable",
    "OrderTable"
]


class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


def convert_decimals(obj):
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
    if not phone:
        return ""
    digits = re.sub(r'\D', '', str(phone))
    if len(digits) == 12 and digits.startswith('91'):
        return digits[2:]
    if len(digits) == 13 and digits.startswith('91'):
        return digits[3:]
    if len(digits) == 10:
        return digits
    return digits[-10:] if len(digits) > 10 else digits


def parse_timestamp(ts: Any) -> Optional[str]:
    if ts is None:
        return None
    if isinstance(ts, (int, float)):
        try:
            if ts > 10000000000:
                ts = ts / 1000
            return datetime.fromtimestamp(ts).isoformat()
        except:
            return None
    if isinstance(ts, str):
        return ts
    return None


def safe_float(value, default=0.0):
    """Safely convert value to float, handling None, empty strings, etc."""
    if value is None or value == '' or value == 'None':
        return default
    try:
        return float(value)
    except (ValueError, TypeError):
        return default


def lambda_handler(event, context):
    """
    Main Lambda handler for scheduled ETL.
    
    Triggered by EventBridge on schedule.
    """
    print(f"ETL Lambda started at {datetime.now().isoformat()}")
    print(f"Event: {json.dumps(event)}")
    
    dynamodb = boto3.resource('dynamodb', region_name=AWS_REGION)
    s3 = boto3.client('s3', region_name=AWS_REGION)
    today = date.today()
    
    results = {
        'extract': {},
        'transform': {},
        'unify': {},
        'status': 'started',
        'timestamp': datetime.now().isoformat()
    }
    
    try:
        # =====================================================================
        # LAYER 1: EXTRACT - DynamoDB -> S3 Raw JSON
        # =====================================================================
        print("=" * 60)
        print("LAYER 1: EXTRACT")
        
        all_data = {}
        
        for table_name in LOYALTY_TABLES:
            items = scan_table_full(dynamodb, table_name)
            all_data[table_name] = items
            
            # Upload to S3 Raw
            s3_path = f"raw/dynamodb/{table_name}/year={today.year}/month={today.month:02d}/day={today.day:02d}/data.json"
            s3.put_object(
                Bucket=S3_BUCKET,
                Key=s3_path,
                Body=json.dumps(items, cls=DecimalEncoder).encode('utf-8'),
                ContentType='application/json'
            )
            results['extract'][table_name] = len(items)
            print(f"  {table_name}: {len(items)} items -> s3://{S3_BUCKET}/{s3_path}")
        
        # =====================================================================
        # LAYER 2: TRANSFORM & LAYER 3: UNIFY
        # =====================================================================
        print("=" * 60)
        print("LAYER 2 & 3: TRANSFORM + UNIFY")
        
        # Build lookups
        users = {u.get('userId', ''): transform_user(u) for u in all_data.get('UserTable', [])}
        wallets = {w.get('userId', ''): transform_wallet(w) for w in all_data.get('WalletTable', [])}
        tiers = {t.get('tierId', ''): transform_tier(t) for t in all_data.get('TierDetailsTable', [])}
        
        # dim_tier
        tier_data = list(tiers.values())
        upload_csv(s3, tier_data, f"processed/unified/loyalty/dim_tier/year={today.year}/month={today.month:02d}/day={today.day:02d}/data.csv")
        results['unify']['dim_tier'] = len(tier_data)
        
        # dim_loyalty_users (joined) - EXPLICIT COLUMN ORDER to match COPY
        dim_users = []
        for user_id, user in users.items():
            wallet = wallets.get(user_id, {})
            tier = tiers.get(user.get('tier_id'), {})
            # Column order MUST match COPY command exactly
            dim_users.append({
                'user_id': user.get('user_id', ''),
                'user_name': user.get('user_name', ''),
                'phone_number': user.get('phone_number', ''),
                'phone_normalized': user.get('phone_normalized', ''),
                'email': user.get('email', ''),
                'tier_id': user.get('tier_id', ''),
                'tier_name': tier.get('tier_name', 'Unknown'),
                'referral_code': user.get('referral_code', ''),
                'remaining_coins': safe_float(wallet.get('remaining_amount', 0)),
                'total_earned': safe_float(wallet.get('total_amount', 0)),
                'total_used': safe_float(wallet.get('used_amount', 0)),
                'signup_date': user.get('signup_date', '')
            })
        upload_csv(s3, dim_users, f"processed/unified/loyalty/dim_loyalty_users/year={today.year}/month={today.month:02d}/day={today.day:02d}/data.csv")
        results['unify']['dim_loyalty_users'] = len(dim_users)
        
        # fact_wallet_transactions
        transactions = [transform_transaction(t) for t in all_data.get('WalletTransactionTable', [])]
        upload_csv(s3, transactions, f"processed/unified/loyalty/fact_wallet_transactions/year={today.year}/month={today.month:02d}/day={today.day:02d}/data.csv")
        results['unify']['fact_wallet_transactions'] = len(transactions)
        
        # fact_referrals
        referrals = [transform_referral(r, users) for r in all_data.get('TierReferralTable', [])]
        upload_csv(s3, referrals, f"processed/unified/loyalty/fact_referrals/year={today.year}/month={today.month:02d}/day={today.day:02d}/data.csv")
        results['unify']['fact_referrals'] = len(referrals)
        
        # fact_leads
        leads = [transform_lead(l, users) for l in all_data.get('LeadTable', [])]
        upload_csv(s3, leads, f"processed/unified/loyalty/fact_leads/year={today.year}/month={today.month:02d}/day={today.day:02d}/data.csv")
        results['unify']['fact_leads'] = len(leads)
        
        # fact_withdrawals
        withdrawals = [transform_withdrawal(w, users) for w in all_data.get('WithdrawnTable', [])]
        upload_csv(s3, withdrawals, f"processed/unified/loyalty/fact_withdrawals/year={today.year}/month={today.month:02d}/day={today.day:02d}/data.csv")
        results['unify']['fact_withdrawals'] = len(withdrawals)
        
        # fact_orders
        orders = [transform_order(o, users) for o in all_data.get('OrderTable', [])]
        upload_csv(s3, orders, f"processed/unified/loyalty/fact_orders/year={today.year}/month={today.month:02d}/day={today.day:02d}/data.csv")
        results['unify']['fact_orders'] = len(orders)
        
        # =====================================================================
        # GENERATE COPY COMMANDS (saved to S3)
        # =====================================================================
        copy_commands = generate_copy_commands(today)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"metadata/runs/loyalty/year={today.year}/month={today.month:02d}/day={today.day:02d}/copy_commands.sql",
            Body=copy_commands.encode('utf-8'),
            ContentType='text/plain'
        )
        
        # =====================================================================
        # LOAD INTO REDSHIFT (using Data API)
        # =====================================================================
        if ENABLE_REDSHIFT_LOAD:
            print("=" * 60)
            print("LAYER 4: LOAD INTO REDSHIFT")
            
            redshift_results = load_to_redshift(today)
            results['redshift_load'] = redshift_results
            
            if redshift_results.get('status') == 'success':
                print("Redshift load completed successfully!")
            else:
                print(f"Redshift load failed: {redshift_results.get('error')}")
        else:
            print("Redshift load disabled. Set ENABLE_REDSHIFT_LOAD=true to enable.")
        
        results['status'] = 'success'
        print("=" * 60)
        print("ETL COMPLETED SUCCESSFULLY")
        print(f"Results: {json.dumps(results, indent=2)}")
        
    except Exception as e:
        results['status'] = 'failed'
        results['error'] = str(e)
        print(f"ETL FAILED: {e}")
        raise
    
    return results


def load_to_redshift(today) -> Dict:
    """
    Load data into Redshift using Data API.
    Executes COPY commands for each loyalty table.
    """
    import time
    
    redshift_data = boto3.client('redshift-data', region_name=AWS_REGION)
    results = {'tables': {}, 'status': 'started'}
    
    # Define COPY statements
    tables = [
        ('dim_tier', 'tier_id, tier_name, redemption_rate'),
        ('dim_loyalty_users', 'user_id, user_name, phone_number, phone_normalized, email, tier_id, tier_name, referral_code, remaining_coins, total_earned, total_used, signup_date'),
        ('fact_wallet_transactions', 'transaction_id, user_id, transaction_type, title, amount, reason, status, created_at'),
        ('fact_referrals', 'referral_id, referrer_user_id, referred_phone, referred_phone_normalized, referral_code, status, created_at, referrer_name, referred_name, referred_user_id'),
        ('fact_leads', 'lead_id, generator_user_id, lead_name, lead_phone, occasion_name, lead_stage, estimated_value, created_at, generator_name'),
        ('fact_withdrawals', 'withdrawal_id, user_id, requested_amount, approved_amount, status, bank_id, upi_id, created_at, processed_at, user_name'),
        ('fact_orders', 'order_id, user_id, user_name, phone_number, grand_total, sub_total, discount, coins_used, order_status, payment_mode, created_at')
    ]
    
    try:
        for table_name, columns in tables:
            s3_path = f"s3://{S3_BUCKET}/processed/unified/loyalty/{table_name}/year={today.year}/month={today.month:02d}/day={today.day:02d}/data.csv"
            
            # Build SQL
            sql = f"""
            TRUNCATE TABLE loyalty.{table_name};
            COPY loyalty.{table_name} ({columns})
            FROM '{s3_path}'
            IAM_ROLE '{REDSHIFT_IAM_ROLE}'
            CSV IGNOREHEADER 1 BLANKSASNULL EMPTYASNULL TIMEFORMAT 'YYYY-MM-DDTHH:MI:SS';
            """
            
            print(f"  Loading {table_name}...")
            
            # Execute using Data API
            execute_params = {
                'Database': REDSHIFT_DATABASE,
                'Sql': sql
            }
            
            # Use cluster ID for provisioned, workgroup for serverless
            if REDSHIFT_WORKGROUP:
                execute_params['WorkgroupName'] = REDSHIFT_WORKGROUP
            else:
                execute_params['ClusterIdentifier'] = REDSHIFT_CLUSTER_ID
                execute_params['DbUser'] = REDSHIFT_USER
            
            response = redshift_data.execute_statement(**execute_params)
            statement_id = response['Id']
            
            # Wait for completion (with timeout)
            max_wait = 300  # 5 minutes max per table
            waited = 0
            while waited < max_wait:
                status_response = redshift_data.describe_statement(Id=statement_id)
                status = status_response['Status']
                
                if status == 'FINISHED':
                    results['tables'][table_name] = 'success'
                    print(f"    [OK] {table_name} loaded successfully")
                    break
                elif status == 'FAILED':
                    error = status_response.get('Error', 'Unknown error')
                    results['tables'][table_name] = f'failed: {error}'
                    print(f"    [ERROR] {table_name}: {error}")
                    break
                elif status in ['PICKED', 'STARTED', 'SUBMITTED']:
                    time.sleep(2)
                    waited += 2
                else:
                    time.sleep(2)
                    waited += 2
            
            if waited >= max_wait:
                results['tables'][table_name] = 'timeout'
                print(f"    [TIMEOUT] {table_name} - took too long")
        
        # Check overall status
        if all(v == 'success' for v in results['tables'].values()):
            results['status'] = 'success'
        else:
            results['status'] = 'partial'
            
    except Exception as e:
        results['status'] = 'failed'
        results['error'] = str(e)
        print(f"Redshift load error: {e}")
    
    return results


def scan_table_full(dynamodb, table_name: str) -> List[Dict]:
    """Scan DynamoDB table with full pagination."""
    table = dynamodb.Table(table_name)
    items = []
    scan_kwargs = {}
    
    while True:
        response = table.scan(**scan_kwargs)
        items.extend(response.get('Items', []))
        
        if 'LastEvaluatedKey' not in response:
            break
        scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
    
    return convert_decimals(items)


def upload_csv(s3, data: List[Dict], s3_path: str):
    """Upload list of dicts to S3 as CSV."""
    if not data:
        return
    
    output = io.StringIO()
    writer = csv.DictWriter(output, fieldnames=data[0].keys())
    writer.writeheader()
    writer.writerows(data)
    
    s3.put_object(
        Bucket=S3_BUCKET,
        Key=s3_path,
        Body=output.getvalue().encode('utf-8'),
        ContentType='text/csv'
    )
    print(f"  Uploaded: s3://{S3_BUCKET}/{s3_path} ({len(data)} rows)")


def transform_user(item: Dict) -> Dict:
    return {
        'user_id': item.get('userId', ''),
        'user_name': item.get('userName', ''),
        'phone_number': item.get('phoneNumber', ''),
        'phone_normalized': normalize_phone(item.get('phoneNumber', '')),
        'email': item.get('emailId', ''),
        'tier_id': item.get('tierId', ''),
        'referral_code': item.get('referralCode', ''),
        'signup_date': parse_timestamp(item.get('created_time'))
    }


def transform_wallet(item: Dict) -> Dict:
    return {
        'wallet_id': item.get('walletId', ''),
        'user_id': item.get('userId', ''),
        'remaining_amount': float(item.get('remainingAmount', 0)),
        'total_amount': float(item.get('totalAmount', 0)),
        'used_amount': float(item.get('usedAmount', 0))
    }


def transform_tier(item: Dict) -> Dict:
    tier_config = {
        'GOLD': {'name': 'Gold', 'rate': 1.0},
        'SILVER': {'name': 'Silver', 'rate': 0.7},
        'BRONZE': {'name': 'Bronze', 'rate': 0.4}
    }
    tier_type = item.get('tierType', 'BRONZE').upper()
    config = tier_config.get(tier_type, {'name': 'Unknown', 'rate': 0.4})
    return {
        'tier_id': item.get('tierId', ''),
        'tier_name': config['name'],
        'redemption_rate': config['rate']
    }


def transform_transaction(item: Dict) -> Dict:
    amount = float(item.get('amount', 0))
    return {
        'transaction_id': item.get('transactionId', ''),
        'user_id': item.get('userId', ''),
        'transaction_type': 'credit' if amount >= 0 else 'debit',
        'title': item.get('title', ''),
        'amount': amount,
        'reason': item.get('reason', ''),
        'status': item.get('status', ''),
        'created_at': parse_timestamp(item.get('created_time'))
    }


def transform_referral(item: Dict, users: Dict) -> Dict:
    referrer = users.get(item.get('userId', ''), {})
    referred_phone_normalized = normalize_phone(item.get('sentTo', ''))
    referred_user = next((u for u in users.values() if u.get('phone_normalized') == referred_phone_normalized), None)
    
    return {
        'referral_id': item.get('tierReferralId', ''),
        'referrer_user_id': item.get('userId', ''),
        'referred_phone': item.get('sentTo', ''),
        'referred_phone_normalized': referred_phone_normalized,
        'referral_code': item.get('appliedCode', ''),
        'status': 'applied' if item.get('appliedCode') else 'pending',
        'created_at': parse_timestamp(item.get('created_time')),
        'referrer_name': referrer.get('user_name', ''),
        'referred_name': referred_user.get('user_name', '') if referred_user else '',
        'referred_user_id': referred_user.get('user_id', '') if referred_user else ''
    }


def transform_lead(item: Dict, users: Dict) -> Dict:
    generator = users.get(item.get('userId', ''), {})
    return {
        'lead_id': item.get('leadId', ''),
        'generator_user_id': item.get('userId', ''),
        'lead_name': item.get('leadName', ''),
        'lead_phone': item.get('leadPhoneNumber', ''),
        'occasion_name': item.get('occasionName', ''),
        'lead_stage': item.get('leadStage', ''),
        'estimated_value': float(item.get('estimatedValue', 0)),
        'created_at': parse_timestamp(item.get('created_time')),
        'generator_name': generator.get('user_name', '')
    }


def transform_withdrawal(item: Dict, users: Dict) -> Dict:
    user = users.get(item.get('userId', ''), {})
    return {
        'withdrawal_id': item.get('requestedId', ''),
        'user_id': item.get('userId', ''),
        'requested_amount': float(item.get('requestedAmount', 0)),
        'approved_amount': float(item.get('approvedAmount', 0)) if item.get('approvedAmount') else None,
        'status': item.get('status', ''),
        'bank_id': item.get('bankId', ''),
        'upi_id': item.get('upiId', ''),
        'created_at': parse_timestamp(item.get('created_time')),
        'processed_at': parse_timestamp(item.get('updated_time')),
        'user_name': user.get('user_name', '')
    }


def transform_order(item: Dict, users: Dict) -> Dict:
    """Transform OrderTable items."""
    user = users.get(item.get('userId', ''), {})
    return {
        'order_id': item.get('orderId', ''),
        'user_id': item.get('userId', ''),
        'user_name': user.get('user_name', ''),
        'phone_number': user.get('phone_number', ''),
        'grand_total': safe_float(item.get('grandTotal', 0)),
        'sub_total': safe_float(item.get('subTotal', 0)),
        'discount': safe_float(item.get('discount', 0)),
        'coins_used': safe_float(item.get('coinsUsed', 0)),
        'order_status': item.get('orderStatus', ''),
        'payment_mode': item.get('paymentMode', ''),
        'created_at': parse_timestamp(item.get('created_time'))
    }


def generate_copy_commands(today) -> str:
    tables = [
        ('dim_tier', 'tier_id, tier_name, redemption_rate'),
        ('dim_loyalty_users', 'user_id, user_name, phone_number, phone_normalized, email, tier_id, tier_name, referral_code, remaining_coins, total_earned, total_used, signup_date'),
        ('fact_wallet_transactions', 'transaction_id, user_id, transaction_type, title, amount, reason, status, created_at'),
        ('fact_referrals', 'referral_id, referrer_user_id, referred_phone, referred_phone_normalized, referral_code, status, created_at, referrer_name, referred_name, referred_user_id'),
        ('fact_leads', 'lead_id, generator_user_id, lead_name, lead_phone, occasion_name, lead_stage, estimated_value, created_at, generator_name'),
        ('fact_withdrawals', 'withdrawal_id, user_id, requested_amount, approved_amount, status, bank_id, upi_id, created_at, processed_at, user_name'),
        ('fact_orders', 'order_id, user_id, user_name, phone_number, grand_total, sub_total, discount, coins_used, order_status, payment_mode, created_at')
    ]
    
    commands = ["-- Auto-generated COPY commands", f"-- Date: {today.isoformat()}", ""]
    
    for table, columns in tables:
        s3_path = f"s3://{S3_BUCKET}/processed/unified/loyalty/{table}/year={today.year}/month={today.month:02d}/day={today.day:02d}/data.csv"
        cmd = f"""TRUNCATE TABLE loyalty.{table};
COPY loyalty.{table} ({columns})
FROM '{s3_path}'
IAM_ROLE '{REDSHIFT_IAM_ROLE}'
CSV IGNOREHEADER 1 BLANKSASNULL EMPTYASNULL TIMEFORMAT 'YYYY-MM-DDTHH:MI:SS';
"""
        commands.append(cmd)
    
    return "\n".join(commands)
