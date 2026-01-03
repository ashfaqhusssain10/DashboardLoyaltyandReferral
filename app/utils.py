"""
Utility functions for common operations
"""
from datetime import datetime


def format_date(value, default: str = 'N/A') -> str:
    """
    Convert various date formats to readable date string (YYYY-MM-DD).
    Handles:
    - Unix timestamps (seconds or milliseconds)
    - ISO date strings
    - None/empty values
    """
    if value is None or value == '':
        return default
    
    # If it's already a string that looks like a date
    if isinstance(value, str):
        # Check if it's a numeric string (timestamp)
        if value.isdigit():
            value = int(value)
        else:
            # Assume it's an ISO date string, take first 10 chars
            return value[:10] if len(value) >= 10 else value
    
    # If it's a number (timestamp)
    if isinstance(value, (int, float)):
        try:
            # Handle milliseconds (if > year 3000 in seconds, it's probably ms)
            if value > 10000000000:
                value = value / 1000
            
            dt = datetime.fromtimestamp(value)
            return dt.strftime('%Y-%m-%d')
        except (ValueError, OSError, OverflowError):
            return default
    
    return default


def format_datetime(value, default: str = 'N/A') -> str:
    """
    Convert various date formats to readable datetime string (YYYY-MM-DD HH:MM).
    """
    if value is None or value == '':
        return default
    
    # If it's already a string that looks like a date
    if isinstance(value, str):
        if value.isdigit():
            value = int(value)
        else:
            return value[:16] if len(value) >= 16 else value
    
    # If it's a number (timestamp)
    if isinstance(value, (int, float)):
        try:
            if value > 10000000000:
                value = value / 1000
            
            dt = datetime.fromtimestamp(value)
            return dt.strftime('%Y-%m-%d %H:%M')
        except (ValueError, OSError, OverflowError):
            return default
    
    return default
