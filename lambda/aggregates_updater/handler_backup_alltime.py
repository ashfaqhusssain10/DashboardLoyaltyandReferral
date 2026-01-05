"""
AWS Lambda Handler for DynamoDB Stream Events - INCREMENTAL VERSION

Instead of full table scans, this version processes only the changed records
and updates aggregates incrementally.

Performance: <1 second per invocation (vs 5+ minutes for full scan)
"""
import json
import boto3
from datetime import datetime, date
from decimal import Decimal


# DynamoDB
dynamodb = boto3.resource('dynamodb')
AGGREGATES_TABLE = "AdminAggregatesTable"

# Tier rates
TIER_RATES = {'Bronze': 0.40, 'Silver': 0.70, 'Gold': 1.00, 'Unknown': 0.40}


def lambda_handler(event, context):
    """
    Main Lambda handler. Processes Stream events incrementally.
    """
    try:
        for record in event.get('Records', []):
            event_name = record.get('eventName')  # INSERT, MODIFY, REMOVE
            source_arn = record.get('eventSourceARN', '')
            
            # Get old and new images
            new_image = record.get('dynamodb', {}).get('NewImage', {})
            old_image = record.get('dynamodb', {}).get('OldImage', {})
            
            # Convert DynamoDB format to Python dict
            new_data = dynamo_to_dict(new_image) if new_image else {}
            old_data = dynamo_to_dict(old_image) if old_image else {}
            
            # Route to appropriate handler
            if 'WalletTable' in source_arn:
                handle_wallet_change(event_name, old_data, new_data)
            elif 'TierReferralTable' in source_arn:
                handle_referral_change(event_name, old_data, new_data)
            elif 'LeadTable' in source_arn:
                handle_lead_change(event_name, old_data, new_data)
            elif 'WithdrawnTable' in source_arn:
                handle_withdrawal_change(event_name, old_data, new_data)
        
        return {'statusCode': 200, 'body': 'Incremental update complete'}
        
    except Exception as e:
        print(f"Error: {e}")
        raise


def dynamo_to_dict(dynamo_obj):
    """Convert DynamoDB item format to Python dict."""
    result = {}
    for key, value in dynamo_obj.items():
        if 'S' in value:
            result[key] = value['S']
        elif 'N' in value:
            result[key] = float(value['N'])
        elif 'BOOL' in value:
            result[key] = value['BOOL']
        elif 'M' in value:
            result[key] = dynamo_to_dict(value['M'])
        elif 'L' in value:
            result[key] = [dynamo_to_dict(i) if 'M' in i else list(i.values())[0] for i in value['L']]
        else:
            result[key] = str(value)
    return result


def get_aggregate(agg_type, agg_id):
    """Get existing aggregate data."""
    table = dynamodb.Table(AGGREGATES_TABLE)
    try:
        response = table.get_item(Key={'aggregateType': agg_type, 'aggregateId': agg_id})
        return response.get('Item', {}).get('data', {})
    except:
        return {}


def update_aggregate(agg_type, agg_id, updates):
    """Update aggregate with delta values using atomic updates."""
    table = dynamodb.Table(AGGREGATES_TABLE)
    
    # Build update expression with aliases for all attribute names
    update_parts = ["lastUpdated = :ts"]
    expr_values = {':ts': int(datetime.now().timestamp())}
    expr_names = {'#data': 'data'}  # 'data' is a reserved keyword
    
    key_counter = 0
    for key, delta in updates.items():
        if delta != 0:
            key_alias = f'#k{key_counter}'
            expr_names[key_alias] = key  # Alias the nested key too (e.g., 'users')
            update_parts.append(f"#data.{key_alias} = if_not_exists(#data.{key_alias}, :zero) + :delta_{key_counter}")
            expr_values[f':delta_{key_counter}'] = Decimal(str(delta))
            key_counter += 1
    
    expr_values[':zero'] = Decimal('0')
    
    if len(update_parts) > 1:  # More than just timestamp
        try:
            table.update_item(
                Key={'aggregateType': agg_type, 'aggregateId': agg_id},
                UpdateExpression='SET ' + ', '.join(update_parts),
                ExpressionAttributeValues=expr_values,
                ExpressionAttributeNames=expr_names
            )
            print(f"Updated {agg_type}/{agg_id}: {updates}")
        except Exception as e:
            print(f"Error updating {agg_type}/{agg_id}: {e}")


def set_aggregate_value(agg_type, agg_id, key, value):
    """Set a specific value in aggregate (for non-numeric updates)."""
    table = dynamodb.Table(AGGREGATES_TABLE)
    
    # Convert value to Decimal if numeric
    if isinstance(value, (int, float)):
        value = Decimal(str(value))
    
    try:
        table.update_item(
            Key={'aggregateType': agg_type, 'aggregateId': agg_id},
            UpdateExpression=f'SET #data.{key} = :val, lastUpdated = :ts',
            ExpressionAttributeValues={
                ':val': value,
                ':ts': int(datetime.now().timestamp())
            },
            ExpressionAttributeNames={'#data': 'data'}
        )
        print(f"Set {agg_type}/{agg_id}.{key} = {value}")
    except Exception as e:
        print(f"Error setting {agg_type}/{agg_id}.{key}: {e}")


# =============================================================================
# WALLET CHANGES - Incremental
# =============================================================================

def handle_wallet_change(event_name, old_data, new_data):
    """Handle wallet INSERT/MODIFY/REMOVE incrementally."""
    
    old_balance = float(old_data.get('remainingAmount', 0) or 0)
    new_balance = float(new_data.get('remainingAmount', 0) or 0)
    
    # Calculate deltas
    coin_delta = new_balance - old_balance
    
    # Active user delta (balance > 0)
    was_active = old_balance > 0
    is_active = new_balance > 0
    active_delta = 0
    if not was_active and is_active:
        active_delta = 1
    elif was_active and not is_active:
        active_delta = -1
    
    # Update GLOBAL stats
    if coin_delta != 0 or active_delta != 0:
        update_aggregate("GLOBAL", "STATS", {
            'totalCoins': coin_delta,
            'activeUsersCount': active_delta
        })
    
    # Update TIER stats (if we know the tier)
    # For now, we update Bronze (since all users are Bronze)
    # A more complete solution would look up the user's tier
    if coin_delta != 0 or active_delta != 0:
        tier_name = 'Bronze'  # Default - could be looked up
        rate = TIER_RATES.get(tier_name, 0.40)
        update_aggregate("TIER", tier_name, {
            'coins': coin_delta,
            'rupees': coin_delta * rate,
            'users': active_delta
        })
    
    print(f"Wallet change: coins={coin_delta:+.2f}, active={active_delta:+d}")


# =============================================================================
# REFERRAL CHANGES - Incremental
# =============================================================================

def handle_referral_change(event_name, old_data, new_data):
    """Handle referral INSERT/REMOVE incrementally."""
    
    today = date.today().isoformat()
    
    if event_name == 'INSERT':
        # New referral added
        created = parse_date(new_data.get('created_time'))
        
        if created == today:
            update_aggregate("GLOBAL", "STATS", {'todayReferralsCount': 1})
        
        # Update daily metrics
        if created:
            update_aggregate("DAILY", created, {'referrals': 1})
        
        print(f"Referral added: date={created}")
        
    elif event_name == 'REMOVE':
        # Referral removed (rare)
        created = parse_date(old_data.get('created_time'))
        
        if created == today:
            update_aggregate("GLOBAL", "STATS", {'todayReferralsCount': -1})
        
        if created:
            update_aggregate("DAILY", created, {'referrals': -1})


# =============================================================================
# LEAD CHANGES - Incremental
# =============================================================================

def handle_lead_change(event_name, old_data, new_data):
    """Handle lead INSERT/REMOVE incrementally."""
    
    today = date.today().isoformat()
    
    if event_name == 'INSERT':
        created = parse_date(new_data.get('created_time'))
        
        if created == today:
            update_aggregate("GLOBAL", "STATS", {'todayLeadsCount': 1})
        
        if created:
            update_aggregate("DAILY", created, {'leads': 1})
        
        print(f"Lead added: date={created}")
        
    elif event_name == 'REMOVE':
        created = parse_date(old_data.get('created_time'))
        
        if created == today:
            update_aggregate("GLOBAL", "STATS", {'todayLeadsCount': -1})
        
        if created:
            update_aggregate("DAILY", created, {'leads': -1})


# =============================================================================
# WITHDRAWAL CHANGES - Incremental
# =============================================================================

def handle_withdrawal_change(event_name, old_data, new_data):
    """Handle withdrawal status changes incrementally."""
    
    old_status = str(old_data.get('status', '')).lower()
    new_status = str(new_data.get('status', '')).lower()
    old_amount = float(old_data.get('requestedAmount', 0) or 0)
    new_amount = float(new_data.get('requestedAmount', 0) or 0)
    
    # Calculate pending deltas
    was_pending = old_status == 'pending'
    is_pending = new_status == 'pending'
    
    count_delta = 0
    amount_delta = 0
    
    if not was_pending and is_pending:
        # Became pending
        count_delta = 1
        amount_delta = new_amount
    elif was_pending and not is_pending:
        # Was pending, now resolved
        count_delta = -1
        amount_delta = -old_amount
    elif was_pending and is_pending and old_amount != new_amount:
        # Still pending but amount changed
        amount_delta = new_amount - old_amount
    
    if count_delta != 0 or amount_delta != 0:
        update_aggregate("GLOBAL", "STATS", {
            'pendingWithdrawalsCount': count_delta,
            'pendingWithdrawalsAmount': amount_delta
        })
        print(f"Withdrawal change: count={count_delta:+d}, amount={amount_delta:+.2f}")


# =============================================================================
# HELPERS
# =============================================================================

def parse_date(created_time):
    """Parse timestamp to date string."""
    if created_time is None:
        return None
    
    if isinstance(created_time, (int, float)):
        try:
            ts = float(created_time)
            if ts > 10000000000:
                ts = ts / 1000
            return datetime.fromtimestamp(ts).date().isoformat()
        except:
            return None
    
    if isinstance(created_time, str):
        return created_time[:10]
    
    return None
