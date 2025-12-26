"""
Job title parser for processing phase.

Cleans and standardizes job titles by:
1. Removing structural markers (quantity prefixes, Roman numerals)
2. Removing gender markers ((A), /O, etc.)
3. Applying generic text cleaning (whitespace, punctuation)

Flow: Specific cleaning → Generic cleaning
"""

import re
from servir.src.processing.parsers.text_parser import clean_text


def clean_job_title(raw_title):
    """
    Clean and standardize a job title.
    
    Process:
    1. Remove quantity prefixes (UN/A, UNA, UN, UNOS, UNAS)
    2. Remove gender markers ((A), (O), /A, /O)
    3. Remove Roman numerals (I, II, III, IV, etc.)
    4. Apply generic text cleaning (trim, quotes, punctuation, spaces)
    
    Args:
        raw_title: Raw job title string from collection database
    
    Returns:
        str: Cleaned job title, or None if empty/invalid
    """
    
    # Handle None or non-string input
    if not raw_title or not isinstance(raw_title, str):
        return None
    
    try:
        cleaned = raw_title
        
        # Step 1: Remove quantity prefixes from start
        # Matches: UN/A, UNA, UN, UNOS, UNAS (case-insensitive)
        cleaned = re.sub(r'^(UN/A|UNA|UNAS|UNOS|UN)\s+', '', cleaned, flags=re.IGNORECASE)
        
        # Step 2: Remove gender markers
        # Matches: (A), (O), /A, /O with optional surrounding spaces
        cleaned = re.sub(r'\s*[(/]([AO])[)]\s*', ' ', cleaned)  # (A) or (O)
        cleaned = re.sub(r'\s*/[AO]\s*', ' ', cleaned)  # /A or /O
        
        # Clean up multiple spaces from gender marker removal
        cleaned = ' '.join(cleaned.split())
        
        # Step 3: Remove Roman numerals from end
        # Matches: I, II, III, IV, V, VI, VII, VIII, IX, X, XL, L, XC, C, CD, D, CM, M
        # Pattern: whitespace + Roman numerals + optional whitespace at end
        # Note: Order matters! Match longer patterns first (CM, CD, XC, XL, IX, IV)
        cleaned = re.sub(r'\s+(CM|CD|XC|XL|IX|IV|[IVX])+\s*', ' ', cleaned, flags=re.IGNORECASE)
        
        # Step 4: Apply generic text cleaning
        # This handles: trim, quotes, punctuation, extra spaces
        cleaned = clean_text(cleaned)
        
        # Handle None or "NO INFO" results from generic cleaning
        if not cleaned or cleaned == 'NO INFO':
            return None
        
        return cleaned
    
    except Exception:
        return None
        
        # Step 4: Apply generic text cleaning
        # This handles: trim, quotes, punctuation, extra spaces
        cleaned = clean_text(cleaned)
        
        # Handle None or "NO INFO" results from generic cleaning
        if not cleaned or cleaned == 'NO INFO':
            return None
        
        return cleaned
    
    except Exception:
        return None