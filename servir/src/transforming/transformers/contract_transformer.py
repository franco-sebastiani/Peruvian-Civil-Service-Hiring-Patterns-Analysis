"""
Contract transformer for transforming phase.

Extracts contract regime and classifies temporal nature from cleaned contract types.
"""

from servir.src.transforming.config.contract_config import CONTRACT_CLASSIFICATIONS


def transform_contract(cleaned_contract_type):
    """
    Transform cleaned contract type into regime and temporal nature.
    
    Takes the standardized contract type from cleaning phase and extracts:
    1. Contract regime (legal framework: D.LEG 1057, D.LEG 728, etc.)
    2. Temporal nature (TEMPORARY, PERMANENT, REPLACEMENT, INDETERMINATE)
    
    Args:
        cleaned_contract_type: Standardized contract type from cleaning phase
    
    Returns:
        dict: {
            'contract_type': str or None (regime),
            'contract_temporal_nature': str or None (temporal classification)
        }
    """
    
    # Handle None or empty input
    if not cleaned_contract_type or not isinstance(cleaned_contract_type, str):
        return {
            'contract_type': None,
            'contract_temporal_nature': None
        }
    
    # Look up classification in mapping
    classification = CONTRACT_CLASSIFICATIONS.get(cleaned_contract_type)
    
    if classification:
        return {
            'contract_type': classification['regime'],
            'contract_temporal_nature': classification['temporal_nature']
        }
    
    # If no match found, return None (transformation failed)
    return {
        'contract_type': None,
        'contract_temporal_nature': None
    }