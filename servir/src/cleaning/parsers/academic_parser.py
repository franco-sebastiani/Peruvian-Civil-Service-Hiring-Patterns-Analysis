"""
Academic profile parser for cleaning phase.

Cleans academic/education requirements text field.
Just cleaning - no classification logic.
"""

from servir.src.cleaning.parsers.text_parser import clean_text


def clean_academic_profile(raw_academic):
    """
    Clean academic/education requirements text field.
    
    Process:
    1. Generic text cleaning (trim, quotes, punctuation)
    2. Standardize spacing
    3. Return cleaned text
    
    Args:
        raw_academic: Raw academic profile text from collection database
    
    Returns:
        str: Cleaned academic profile text, or None if empty/invalid
    """
    
    # Step 1: Generic text cleaning
    cleaned = clean_text(raw_academic)
    
    # Handle None or "NO INFO" results
    if not cleaned or cleaned == 'NO INFO':
        return None
    
    try:
        # Step 2: Remove extra spaces
        cleaned = ' '.join(cleaned.split())
        
        return cleaned
    
    except Exception:
        return None