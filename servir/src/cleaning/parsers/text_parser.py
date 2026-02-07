"""
Generic text cleaner for cleaning phase.

Provides core text cleaning functionality used by all field-specific parsers.
Handles: whitespace, quotes, punctuation, extra spaces.

Field-specific parsers (job_title_parser.py, knowledge_parser.py, etc.)
call clean_text() first, then apply their own specialized logic.
"""

import re
from servir.src.cleaning.config.text_config import TEXT_CLEANING_RULES


def clean_text(text_str, trim=True, remove_quotes=True, remove_punctuation=True, remove_extra_spaces=True):
    """
    Generic text cleaning applied to all text fields.
    
    Core operations (always applied):
    - Validates input (None/empty checks)
    - Trims whitespace (if enabled)
    - Removes quotes (if enabled)
    - Removes inverted punctuation ¿, ¡ (if enabled)
    - Removes leading bullets/dashes (if enabled)
    - Removes extra spaces (if enabled)
    
    Does NOT apply field-specific logic. For that, use field-specific parsers
    like job_title_parser.py, knowledge_parser.py, etc.
    
    Args:
        text_str: Raw text string from database
        trim: If True, strip leading/trailing whitespace
        remove_quotes: If True, remove quote characters
        remove_punctuation: If True, remove inverted punctuation (¿, ¡) and bullets
        remove_extra_spaces: If True, collapse multiple spaces to single space
    
    Returns:
        str: Cleaned text, or 'NO INFO' if empty after cleaning, or None on error
    """
    
    # Handle None or non-string input
    if not text_str or not isinstance(text_str, str):
        return None
    
    try:
        cleaned = text_str
        
        # Step 1: Trim whitespace
        if trim:
            cleaned = cleaned.strip()
        
        # Step 2: Remove quotes (leading and trailing)
        if remove_quotes:
            cleaned = cleaned.strip('"\'')
        
        # Step 3: Remove inverted punctuation (¿, ¡) and leading punctuation from start and end
        if remove_punctuation:
            cleaned = re.sub(r'^[¿¡\s]+', '', cleaned)
            cleaned = re.sub(r'[¿¡\s]+$', '', cleaned)
            
            # Remove leading bullets/dashes/periods followed by whitespace
            # Matches: "- ", "– ", "• ", "* ", ". " at start of lines
            cleaned = re.sub(r'^[\s\-–•*.]+', '', cleaned, flags=re.MULTILINE)
        
        # Step 4: Final trim after punctuation removal
        cleaned = cleaned.strip()
        
        # Step 5: Remove extra spaces
        if remove_extra_spaces:
            # Replace multiple spaces with single space
            cleaned = ' '.join(cleaned.split())
        
        # Handle empty result
        if not cleaned:
            return 'NO INFO'
        
        return cleaned
    
    except Exception:
        return None