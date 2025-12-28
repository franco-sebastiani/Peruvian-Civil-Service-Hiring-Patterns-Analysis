"""
Contract type parser for cleaning phase.

Converts raw contract type strings into standardized categories using pattern matching.
"""

import re
from servir.src.cleaning.config.contract_config import CONTRACT_TYPE_PATTERNS


def clean_contract_type(raw_contract_type):
    """
    Clean raw contract type string into standardized category.
    
    Uses regex patterns defined in config/contract_config.py to map raw values
    to standardized contract type categories.
    
    Args:
        raw_contract_type: Raw contract type string from extracting database
    
    Returns:
        dict: {
            'contract_type': str or None,
            'error': str or None
        }
    """
    
    # Handle None or empty input
    if not raw_contract_type or not isinstance(raw_contract_type, str):
        return {
            'contract_type': None,
            'error': 'Contract type is empty or invalid type'
        }
    
    # Try to match against patterns
    cleaned = raw_contract_type.strip()
    
    for pattern, standardized in CONTRACT_TYPE_PATTERNS:
        if re.match(pattern, cleaned):
            return {
                'contract_type': standardized,
                'error': None
            }
    
    # No match found
    return {
        'contract_type': None,
        'error': f'Contract type did not match any pattern: {raw_contract_type}'
    }