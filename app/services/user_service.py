"""
User Service - User operations from UserTable
"""
from typing import Optional, List, Dict
from .dynamodb_service import db_service


TABLE_NAME = "UserTable"


def get_user_by_id(user_id: str) -> Optional[Dict]:
    """Get a user by their ID."""
    return db_service.get_item(TABLE_NAME, {"userId": user_id})


def search_by_phone(phone_number: str) -> List[Dict]:
    """Search users by phone number using GSI."""
    return db_service.query_by_index(
        TABLE_NAME,
        "phoneNumberIndex",
        "phoneNumber",
        phone_number
    )


def search_by_email(email: str) -> List[Dict]:
    """Search users by email using GSI."""
    return db_service.query_by_index(
        TABLE_NAME,
        "emailIndex",
        "emailId",
        email
    )


def get_all_users(limit: int = 100) -> List[Dict]:
    """Get all users (with limit)."""
    return db_service.scan_all(TABLE_NAME, limit)


def search_users(query: str) -> List[Dict]:
    """Search users by ID, phone number, or name."""
    query = query.strip()
    
    if not query:
        return []
    
    # Try phone first (exact match)
    results = search_by_phone(query)
    if results:
        return results
    
    # Try with +91 prefix if not present
    if not query.startswith('+') and query.isdigit():
        results = search_by_phone(f"+91{query}")
        if results:
            return results
    
    # Try without +91 prefix if present
    if query.startswith('+91'):
        results = search_by_phone(query[3:])
        if results:
            return results
    
    # Try as user ID (exact match)
    user = get_user_by_id(query)
    if user:
        return [user]
    
    # Name search removed - scanning 184K users is not practical
    # Use phone number, email, or user ID for search instead
    
    return []

