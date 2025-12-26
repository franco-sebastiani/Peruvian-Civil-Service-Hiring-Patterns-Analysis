"""
Job title parser for processing phase.

Cleans and standardizes job titles by:
1. Generic text cleaning (whitespace, punctuation)
2. Removing structural markers (quantity prefixes, Roman numerals)
3. Removing gender markers ((A), /O, etc.)
"""

import re
from servir.src.processing.parsers.text_parser import clean_text


def clean_job_title(raw_title):
    """
    Clean and standardize a job title.
    
    Process:
    1. Apply generic text cleaning (trim, quotes, punctuation)
    2. Remove quantity prefixes (UN/A, UNA, UN, UNOS, UNAS)
    3. Remove gender markers ((A), (O), /A, /O)
    4. Remove Roman numerals (I, II, III, IV, etc.)
    
    Args:
        raw_title: Raw job title string from collection database
    
    Returns:
        str: Cleaned job title, or None if empty/invalid
    """
    
    # Step 1: Generic text cleaning
    cleaned = clean_text(raw_title)
    
    # Handle None or "NO INFO" results
    if not cleaned or cleaned == 'NO INFO':
        return None
    
    try:
        # Step 2: Remove quantity prefixes from start
        # Matches: UN/A, UNA, UN, UNOS, UNAS (case-insensitive)
        cleaned = re.sub(r'^(UN/A|UNA|UNAS|UNOS|UN)\s+', '', cleaned, flags=re.IGNORECASE)
        
        # Step 3: Remove gender markers
        # Matches: (A), (O), /A, /O with optional surrounding spaces
        cleaned = re.sub(r'\s*[(/]([AO])[)]\s*', ' ', cleaned)  # (A) or (O)
        cleaned = re.sub(r'\s*/[AO]\s*', ' ', cleaned)  # /A or /O
        
        # Clean up multiple spaces from gender marker removal
        cleaned = ' '.join(cleaned.split())
        
        # Step 4: Remove Roman numerals from end
        # Matches: I, II, III, IV, V, X, L, C, D, M, etc.
        # Pattern: whitespace + Roman numerals + optional whitespace at end
        cleaned = re.sub(r'\s+(I+|IV|V|IX|X|XL|L|XC|C|CD|D|CM|M)+\s*$', '', cleaned, flags=re.IGNORECASE)
        
        # Final trim
        cleaned = cleaned.strip()
        
        # Handle empty result after all removals
        if not cleaned:
            return None
        
        return cleaned
    
    except Exception:
        return None