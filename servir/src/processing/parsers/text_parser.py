"""
Text parser for processing phase.

Cleans text fields: trim whitespace, remove extra spaces, fix encoding.
"""

from servir.src.processing.config.text_config import TEXT_CLEANING_RULES


def transform_text(text_str):
    """
    Clean raw text string by removing whitespace and fixing formatting.
    
    Uses rules defined in config/text_rules.py.
    
    Args:
        text_str: Raw text string from SERVIR collection database
    
    Returns:
        dict: {
            'text': str or None,
            'error': str or None
        }
    
    Examples:
        transform_text("  Some Text  ") → {
            'text': 'Some Text',
            'error': None
        }
        
        transform_text("Text  with   extra    spaces") → {
            'text': 'Text with extra spaces',
            'error': None
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
        
        if TEXT_CLEANING_RULES['remove_extra_spaces']:
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