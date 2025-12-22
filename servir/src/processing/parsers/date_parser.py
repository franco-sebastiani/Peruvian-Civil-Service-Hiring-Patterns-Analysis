"""
Date parser for processing phase.

Converts raw date strings from SERVIR into standardized ISO format.
"""

from datetime import datetime


def transform_date(date_str):
    """
    Transform raw date string into ISO format (YYYY-MM-DD).
    
    Handles SERVIR format: "DD/MM/YYYY" (e.g., "19/12/2025")
    
    Args:
        date_str: Raw date string from SERVIR collection database
    
    Returns:
        dict: {
            'date_iso': str or None,
            'error': str or None
        }
    """
    
    # Handle None or empty input
    if not date_str or not isinstance(date_str, str):
        return {
            'date_iso': None,
            'error': 'Date is empty or invalid type'
        }
    
    try:
        # Strip whitespace
        cleaned = date_str.strip()
        
        # Parse DD/MM/YYYY format
        date_obj = datetime.strptime(cleaned, "%d/%m/%Y")
        
        # Convert to ISO format (YYYY-MM-DD)
        date_iso = date_obj.strftime("%Y-%m-%d")
        
        return {
            'date_iso': date_iso,
            'error': None
        }
    
    except ValueError as e:
        return {
            'date_iso': None,
            'error': f'Failed to parse date string: {str(e)}'
        }
    
    except Exception as e:
        return {
            'date_iso': None,
            'error': f'Unexpected error parsing date: {str(e)}'
        }