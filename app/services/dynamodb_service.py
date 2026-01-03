"""
DynamoDB Service - Core connection and utilities
"""
import boto3
from boto3.dynamodb.conditions import Key, Attr
from botocore.exceptions import ClientError
from decimal import Decimal
import json
from datetime import datetime
from typing import Optional, List, Dict, Any


def convert_decimals(obj):
    """Recursively convert Decimal objects to int/float."""
    if isinstance(obj, list):
        return [convert_decimals(i) for i in obj]
    elif isinstance(obj, dict):
        return {k: convert_decimals(v) for k, v in obj.items()}
    elif isinstance(obj, Decimal):
        # Convert to int if it's a whole number, otherwise float
        if obj % 1 == 0:
            return int(obj)
        return float(obj)
    return obj


class DecimalEncoder(json.JSONEncoder):
    """Handle Decimal types from DynamoDB."""
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super().default(obj)


class DynamoDBService:
    """Base service for DynamoDB operations."""
    
    def __init__(self):
        self.dynamodb = boto3.resource('dynamodb')
        self.client = boto3.client('dynamodb')
    
    def get_table(self, table_name: str):
        """Get a DynamoDB table resource."""
        return self.dynamodb.Table(table_name)
    
    def scan_all(self, table_name: str, limit: int = None) -> List[Dict]:
        """
        Scan a table with full DynamoDB pagination.
        If limit is provided, returns at most 'limit' items.
        If limit is None, returns ALL items (use with caution on large tables).
        """
        table = self.get_table(table_name)
        items = []
        scan_kwargs = {}
        page_count = 0
        
        while True:
            response = table.scan(**scan_kwargs)
            items.extend(response.get('Items', []))
            page_count += 1
            
            # Stop if limit reached
            if limit and len(items) >= limit:
                items = items[:limit]
                break
            
            # Stop if no more pages
            if 'LastEvaluatedKey' not in response:
                break
            
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        
        # Debug log
        print(f"[DEBUG] scan_all({table_name}): {len(items)} items from {page_count} page(s)")
        
        return convert_decimals(items)
    
    def scan_all_paginated(self, table_name: str, page: int = 1, limit: int = 50) -> Dict:
        """
        Scan table and return paginated results.
        Matches senior's API format.
        
        Returns: {
            'paginatedItems': List[Dict],  # Items for current page
            'totalItems': int,              # Total count
            'page': int,                    # Current page
            'limit': int                    # Items per page
        }
        """
        table = self.get_table(table_name)
        all_items = []
        scan_kwargs = {}
        
        # Fetch all items (with DynamoDB pagination)
        while True:
            response = table.scan(**scan_kwargs)
            all_items.extend(response.get('Items', []))
            
            if 'LastEvaluatedKey' not in response:
                break
            scan_kwargs['ExclusiveStartKey'] = response['LastEvaluatedKey']
        
        total_items = len(all_items)
        
        # Pagination logic (same as senior's approach)
        start_index = (page - 1) * limit
        paginated_items = all_items[start_index:start_index + limit]
        
        return {
            'paginatedItems': convert_decimals(paginated_items),
            'totalItems': total_items,
            'page': page,
            'limit': limit
        }
    
    def query_by_index(
        self, 
        table_name: str, 
        index_name: str, 
        key_name: str, 
        key_value: str
    ) -> List[Dict]:
        """Query a table using a GSI."""
        table = self.get_table(table_name)
        response = table.query(
            IndexName=index_name,
            KeyConditionExpression=Key(key_name).eq(key_value)
        )
        return convert_decimals(response.get('Items', []))
    
    def get_item(self, table_name: str, key: Dict) -> Optional[Dict]:
        """Get a single item by primary key."""
        table = self.get_table(table_name)
        response = table.get_item(Key=key)
        item = response.get('Item')
        return convert_decimals(item) if item else None
    
    def update_item(
        self, 
        table_name: str, 
        key: Dict, 
        update_expression: str,
        expression_values: Dict
    ) -> Dict:
        """Update an item."""
        table = self.get_table(table_name)
        response = table.update_item(
            Key=key,
            UpdateExpression=update_expression,
            ExpressionAttributeValues=expression_values,
            ReturnValues="ALL_NEW"
        )
        return convert_decimals(response.get('Attributes', {}))


# Singleton instance
db_service = DynamoDBService()

