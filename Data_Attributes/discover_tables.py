"""
DynamoDB Discovery Script
--------------------------
Lists all DynamoDB tables and their attributes.
Run this to understand the data structure before building the dashboard.

Usage:
    python discover_tables.py

Prerequisites:
    - AWS credentials configured (via environment variables or ~/.aws/credentials)
    - boto3 installed: pip install boto3
"""

import boto3
from botocore.exceptions import ClientError, NoCredentialsError
import json


def get_dynamodb_client():
    """Create DynamoDB client with error handling."""
    try:
        dynamodb = boto3.client('dynamodb')
        # Test connection by listing tables
        dynamodb.list_tables(Limit=1)
        return dynamodb
    except NoCredentialsError:
        print("❌ AWS credentials not found!")
        print("\nPlease set up your credentials using one of these methods:")
        print("1. Environment variables:")
        print("   set AWS_ACCESS_KEY_ID=your_access_key")
        print("   set AWS_SECRET_ACCESS_KEY=your_secret_key")
        print("   set AWS_DEFAULT_REGION=your_region (e.g., ap-south-1)")
        print("\n2. AWS credentials file (~/.aws/credentials)")
        print("\n3. AWS CLI: aws configure")
        return None
    except ClientError as e:
        print(f"❌ AWS Error: {e}")
        return None


def list_all_tables(dynamodb):
    """List all DynamoDB tables in the account."""
    tables = []
    paginator = dynamodb.get_paginator('list_tables')
    
    for page in paginator.paginate():
        tables.extend(page['TableNames'])
    
    return tables


def get_table_details(dynamodb, table_name):
    """Get detailed information about a table."""
    try:
        response = dynamodb.describe_table(TableName=table_name)
        table = response['Table']
        
        details = {
            'name': table_name,
            'status': table['TableStatus'],
            'item_count': table.get('ItemCount', 'N/A'),
            'size_bytes': table.get('TableSizeBytes', 'N/A'),
            'key_schema': [],
            'attributes': [],
            'gsi': [],
            'lsi': []
        }
        
        # Key Schema
        for key in table['KeySchema']:
            key_type = 'Partition Key' if key['KeyType'] == 'HASH' else 'Sort Key'
            details['key_schema'].append({
                'name': key['AttributeName'],
                'type': key_type
            })
        
        # Attribute Definitions
        for attr in table['AttributeDefinitions']:
            attr_type = {
                'S': 'String',
                'N': 'Number',
                'B': 'Binary'
            }.get(attr['AttributeType'], attr['AttributeType'])
            details['attributes'].append({
                'name': attr['AttributeName'],
                'type': attr_type
            })
        
        # Global Secondary Indexes
        for gsi in table.get('GlobalSecondaryIndexes', []):
            details['gsi'].append({
                'name': gsi['IndexName'],
                'keys': [k['AttributeName'] for k in gsi['KeySchema']]
            })
        
        # Local Secondary Indexes
        for lsi in table.get('LocalSecondaryIndexes', []):
            details['lsi'].append({
                'name': lsi['IndexName'],
                'keys': [k['AttributeName'] for k in lsi['KeySchema']]
            })
        
        return details
    
    except ClientError as e:
        print(f"  ⚠️ Error getting details for {table_name}: {e}")
        return None


def get_sample_items(table_name, limit=3):
    """Get sample items from a table to see actual data structure."""
    try:
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(table_name)
        response = table.scan(Limit=limit)
        return response.get('Items', [])
    except ClientError as e:
        print(f"  ⚠️ Could not scan {table_name}: {e}")
        return []


def print_table_summary(details, show_samples=False):
    """Print a formatted summary of a table."""
    print(f"\n{'='*60}")
    print(f"📊 TABLE: {details['name']}")
    print(f"{'='*60}")
    print(f"   Status: {details['status']}")
    print(f"   Items: {details['item_count']:,}" if isinstance(details['item_count'], int) else f"   Items: {details['item_count']}")
    
    print(f"\n   🔑 Key Schema:")
    for key in details['key_schema']:
        print(f"      - {key['name']} ({key['type']})")
    
    print(f"\n   📋 Attributes:")
    for attr in details['attributes']:
        print(f"      - {attr['name']}: {attr['type']}")
    
    if details['gsi']:
        print(f"\n   🔍 Global Secondary Indexes:")
        for gsi in details['gsi']:
            print(f"      - {gsi['name']}: {gsi['keys']}")
    
    if details['lsi']:
        print(f"\n   📑 Local Secondary Indexes:")
        for lsi in details['lsi']:
            print(f"      - {lsi['name']}: {lsi['keys']}")
    
    if show_samples:
        samples = get_sample_items(details['name'])
        if samples:
            print(f"\n   📝 Sample Item Keys/Attributes:")
            all_keys = set()
            for item in samples:
                all_keys.update(item.keys())
            for key in sorted(all_keys):
                print(f"      - {key}")


def main():
    print("🔍 DynamoDB Table Discovery")
    print("=" * 60)
    
    # Connect to DynamoDB
    dynamodb = get_dynamodb_client()
    if not dynamodb:
        return
    
    # List all tables
    print("\n📡 Connecting to AWS DynamoDB...")
    tables = list_all_tables(dynamodb)
    
    if not tables:
        print("❌ No tables found in this region.")
        print("   Try setting AWS_DEFAULT_REGION to a different region.")
        return
    
    print(f"\n✅ Found {len(tables)} table(s):\n")
    for i, table in enumerate(tables, 1):
        print(f"   {i}. {table}")
    
    # Get details for each table
    print("\n" + "=" * 60)
    print("📊 TABLE DETAILS")
    print("=" * 60)
    
    all_details = []
    txt_output = []  # Collect output for txt file
    
    txt_output.append("DynamoDB Table Discovery Report")
    txt_output.append("=" * 60)
    txt_output.append(f"\nTotal Tables Found: {len(tables)}\n")
    
    for table_name in tables:
        details = get_table_details(dynamodb, table_name)
        if details:
            all_details.append(details)
            print_table_summary(details, show_samples=True)
            
            # Build txt output for this table
            txt_output.append(f"\n{'='*60}")
            txt_output.append(f"TABLE: {details['name']}")
            txt_output.append(f"{'='*60}")
            txt_output.append(f"Status: {details['status']}")
            txt_output.append(f"Items: {details['item_count']:,}" if isinstance(details['item_count'], int) else f"Items: {details['item_count']}")
            
            txt_output.append(f"\nKey Schema:")
            for key in details['key_schema']:
                txt_output.append(f"  - {key['name']} ({key['type']})")
            
            txt_output.append(f"\nAttributes:")
            for attr in details['attributes']:
                txt_output.append(f"  - {attr['name']}: {attr['type']}")
            
            if details['gsi']:
                txt_output.append(f"\nGlobal Secondary Indexes:")
                for gsi in details['gsi']:
                    txt_output.append(f"  - {gsi['name']}: {gsi['keys']}")
            
            if details['lsi']:
                txt_output.append(f"\nLocal Secondary Indexes:")
                for lsi in details['lsi']:
                    txt_output.append(f"  - {lsi['name']}: {lsi['keys']}")
            
            # Add sample attributes
            samples = get_sample_items(details['name'])
            if samples:
                all_keys = set()
                for item in samples:
                    all_keys.update(item.keys())
                txt_output.append(f"\nAll Attributes Found in Sample Data:")
                for key in sorted(all_keys):
                    txt_output.append(f"  - {key}")
    
    # Save to JSON for reference
    json_file = 'dynamodb_schema.json'
    with open(json_file, 'w') as f:
        json.dump(all_details, f, indent=2, default=str)
    
    # Save to TXT for easy reading
    txt_file = 'dynamodb_discovery_output.txt'
    with open(txt_file, 'w', encoding='utf-8') as f:
        f.write('\n'.join(txt_output))
    
    print(f"\n{'='*60}")
    print(f"✅ Output saved to:")
    print(f"   - {json_file} (structured data)")
    print(f"   - {txt_file} (human-readable)")
    print(f"{'='*60}")
    print("\n📌 Next Steps:")
    print("   1. Review the tables in dynamodb_discovery_output.txt")
    print("   2. Tell me which tables you want to use for Admin Tower")
    print("   3. I'll update the PRD and create the data layer")


if __name__ == "__main__":
    main()
