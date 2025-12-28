"""
Date parser for cleaning phase.

Converts raw date strings from SERVIR into standardized ISO format.
"""

from datetime import datetime


def clean_date(date_str):
    """
    Clean raw date string into ISO format (YYYY-MM-DD).
    
    Args:
        date_str: Raw date string from extracting database
    
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