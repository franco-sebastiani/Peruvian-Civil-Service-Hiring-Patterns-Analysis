"""
Text parser for processing phase.

Cleans text fields: trim whitespace, remove quotes, remove special punctuation.
Optionally removes Roman numerals (useful for job titles like "ASISTENTE II").
"""

import re
from servir.src.processing.config.text_config import TEXT_CLEANING_RULES


def transform_text(text_str, remove_roman_numerals=False):
    """
    Clean raw text string by removing whitespace and unwanted punctuation.
    
    Removes: quotes, leading dashes/bullets, inverted punctuation (¿, ¡)
    Optionally removes: Roman numerals from end (I, II, III, IV, V, etc.)
    Keeps: actual text content
    
    Uses rules defined in config/text_rules.py.
    
    Args:
        text_str: Raw text string from SERVIR collection database
        remove_roman_numerals: If True, removes Roman numerals from the end
                              (useful for job titles like "ASISTENTE II")
    
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
        
        # Remove inverted punctuation (¿, ¡) from start and end
        cleaned = re.sub(r'^[¿¡\s]+', '', cleaned)
        cleaned = re.sub(r'[¿¡\s]+$', '', cleaned)
        
        # Remove leading bullets/dashes followed by whitespace
        # Matches: "- ", "– ", "• ", "* " at start of lines
        cleaned = re.sub(r'^[\s\-–•*]+', '', cleaned, flags=re.MULTILINE)
        
        # Clean up the result
        cleaned = cleaned.strip()
        
        if TEXT_CLEANING_RULES['remove_extra_spaces']:
            # Replace multiple spaces with single space
            cleaned = ' '.join(cleaned.split())
        
        # Remove Roman numerals from the end if requested
        if remove_roman_numerals:
            cleaned = re.sub(r'\s+(I+|IV|V|IX|X|XL|L|XC|C|CD|D|CM|M)+\s*$', '', cleaned, flags=re.IGNORECASE)
        
        # Clean up again after Roman numeral removal
        cleaned = cleaned.strip()
        
        # Handle empty result after cleaning
        if not cleaned:
            return {
                'text': 'NO INFO',
                'error': None
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