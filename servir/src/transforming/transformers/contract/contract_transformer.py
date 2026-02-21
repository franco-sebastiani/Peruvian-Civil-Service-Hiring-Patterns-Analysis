"""
Contract transformer for transforming phase.

Extracts contract regime and classifies temporal nature from cleaned contract types.

Uses JSON configuration file for mappings (industry standard for small config data).
"""

import json
from pathlib import Path


# Load classifications from JSON (loaded once at module import)
_config_path = Path(__file__).parent / "contract_classifications.json"

with open(_config_path, 'r', encoding='utf-8') as f:
    CONTRACT_CLASSIFICATIONS = json.load(f)


def transform_contract(cleaned_contract_type):
    """
    Transform cleaned contract type into regime and temporal nature.
    
    Takes the standardized contract type from cleaning phase and extracts:
    1. Contract regime (legal framework: D.LEG 1057, D.LEG 728, etc.)
    2. Temporal nature (TEMPORARY, PERMANENT, REPLACEMENT, INDETERMINATE)
    
    Args:
        cleaned_contract_type (str): Standardized contract type from cleaning phase
    
    Returns:
        dict: {
            'contract_type': str or None (regime),
            'contract_temporal_nature': str or None (temporal classification)
        }
    
    Example:
        >>> transform_contract("D.LEG 1057 DETERMINADO SUPLENCIA")
        {'contract_type': 'D.LEG 1057', 'contract_temporal_nature': 'REPLACEMENT'}
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
    
    # If no match found, return None (unknown contract type)
    return {
        'contract_type': None,
        'contract_temporal_nature': None
    }


def transform_batch(contract_types):
    """
    Transform multiple contract types.
    
    Args:
        contract_types (list): List of contract type strings
    
    Returns:
        list of dicts: Transformed results for each input
    """
    return [transform_contract(ct) for ct in contract_types]


# Example usage and testing
if __name__ == "__main__":
    print("Testing contract transformer...")
    print("=" * 60)
    
    # Test each contract type
    test_cases = [
        "D.LEG 1057 DETERMINADO NECESIDAD TRANSITORIA",
        "D.LEG 1057 DETERMINADO SUPLENCIA",
        "D.LEG 1057 INDETERMINADO",
        "D.LEG 728",
        "D.LEG 276",
        "DOCENTES UNIVERSITARIOS LEY 30220",
        "LEY 30057",
        None,  # Test null handling
        "UNKNOWN TYPE"  # Test unknown value
    ]
    
    for test_input in test_cases:
        result = transform_contract(test_input)
        print(f"\nInput: {test_input}")
        print(f"  Regime: {result['contract_type']}")
        print(f"  Temporal: {result['contract_temporal_nature']}")
    
    print("\n" + "=" * 60)
    print("✓ All tests complete")