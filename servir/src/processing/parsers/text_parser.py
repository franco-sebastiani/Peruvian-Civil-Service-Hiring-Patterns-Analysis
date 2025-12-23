"""
Text parser for processing phase.

Cleans text fields: trim whitespace, remove quotes, remove bullets/dashes, fix spacing.
"""

import re
from servir.src.processing.config.text_config import TEXT_CLEANING_RULES


def transform_text(text_str):
    """
    Clean raw text string by removing whitespace, quotes, and bullet points.
    
    Uses rules defined in config/text_rules.py.
    
    Args:
        text_str: Raw text string from SERVIR collection database
    
    Returns:
        dict: {
            'text': str or None,
            'error': str or None
        }
    """
    
    # Handle None or empty input
    if not text_str or not isinstance(text_str, str):
        return {
            'text': None,
            'error': 'Text is empty or invalid type'
        }
    
    try:
        cleaned = text_str
        
        # Apply rules
        if TEXT_CLEANING_RULES['trim']:
            cleaned = cleaned.strip()
        
        # Remove quotes (leading and trailing)
        cleaned = cleaned.strip('"\'')
        
        # Remove leading bullets/dashes
        # Matches: "- ", "• ", "* " at start of lines
        cleaned = re.sub(r'^[\s\-•*]+', '', cleaned, flags=re.MULTILINE)
        cleaned = cleaned.strip()
        
        if TEXT_CLEANING_RULES['remove_extra_spaces']:
            # Replace multiple spaces with single space
            cleaned = ' '.join(cleaned.split())
        
        # Handle empty result after cleaning
        if not cleaned:
            return {
                'text': None,
                'error': 'Text is empty after cleaning'
            }
        
        return {
            'text': cleaned,
            'error': None
        }
    
    except Exception as e:
        return {
            'text': None,
            'error': f'Failed to clean text: {str(e)}'
        }