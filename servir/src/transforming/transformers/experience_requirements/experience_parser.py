"""
Experience Parser - Extracts structured fields from experience requirements.

Uses regex patterns to extract:
- General experience years
- Specific experience years
- Sector requirements (public/private as dummies)
"""

import re
import json
from pathlib import Path


# Load patterns from JSON
_patterns_path = Path(__file__).parent / "experience_patterns.json"
with open(_patterns_path, 'r', encoding='utf-8') as f:
    PATTERNS = json.load(f)


def extract_general_years(text):
    """
    Extract general experience years from text.
    
    Args:
        text (str): Experience requirement text
    
    Returns:
        int or None: Years of general experience
    """
    if not text:
        return None
    
    text = text.upper()
    
    # Try each pattern
    for pattern in PATTERNS['general_years_patterns']:
        match = re.search(pattern, text)
        if match:
            years = int(match.group(1))
            return years
    
    return None


def extract_specific_years(text):
    """
    Extract specific experience years from text.
    
    Args:
        text (str): Experience requirement text
    
    Returns:
        int or None: Years of specific experience
    """
    if not text:
        return None
    
    text = text.upper()
    
    # Try each pattern
    for pattern in PATTERNS['specific_years_patterns']:
        match = re.search(pattern, text)
        if match:
            years = int(match.group(1))
            return years
    
    return None


def extract_sector_requirements(text):
    """
    Extract sector requirements as dummy variables.
    
    Args:
        text (str): Experience requirement text
    
    Returns:
        tuple: (sector_public_required, sector_private_required) as (0/1, 0/1)
    """
    if not text:
        return (0, 0)
    
    text = text.upper()
    
    # Check for "no experience" first
    for keyword in PATTERNS['no_experience_keywords']:
        if keyword in text:
            return (0, 0)
    
    # Check for public sector
    public_required = 0
    for keyword in PATTERNS['sector_public_keywords']:
        if keyword in text:
            public_required = 1
            break
    
    # Check for private sector
    private_required = 0
    for keyword in PATTERNS['sector_private_keywords']:
        if keyword in text:
            private_required = 1
            break
    
    return (public_required, private_required)


def parse_experience(text):
    """
    Parse experience requirement text into structured fields.
    
    Args:
        text (str): Experience requirement text
    
    Returns:
        dict: {
            'experience_general_years': int or None,
            'experience_specific_years': int or None,
            'sector_public_required': 0 or 1,
            'sector_private_required': 0 or 1
        }
    """
    if not text or not isinstance(text, str):
        return {
            'experience_general_years': None,
            'experience_specific_years': None,
            'sector_public_required': 0,
            'sector_private_required': 0
        }
    
    # Extract all fields
    general_years = extract_general_years(text)
    specific_years = extract_specific_years(text)
    public_req, private_req = extract_sector_requirements(text)
    
    return {
        'experience_general_years': general_years,
        'experience_specific_years': specific_years,
        'sector_public_required': public_req,
        'sector_private_required': private_req
    }


def parse_batch(experience_texts):
    """
    Parse multiple experience requirements.
    
    Args:
        experience_texts (list): List of experience requirement strings
    
    Returns:
        list of dicts: Parsed results for each input
    """
    return [parse_experience(text) for text in experience_texts]


# Testing
if __name__ == "__main__":
    print("Testing Experience Parser")
    print("=" * 80)
    
    test_cases = [
        "EXPERIENCIA GENERAL: TRES (03) AÑOS EXPERIENCIA ESPECÍFICA: DOS AÑOS EN SECTOR PÚBLICO",
        "EXPERIENCIA LABORAL GENERAL DE 01 AÑO EN EL SECTOR PÚBLICO Y/O PRIVADO",
        "SIN EXPERIENCIA",
        "EXPERIENCIA MINIMA DE 06 MESES EN EL SECTOR PUBLICO Y/O PRIVADO",
        None,
        "EXPERIENCIA GENERAL: NO MENOR A TRES AÑOS EN SECTOR PUBLICO O PRIVADO"
    ]
    
    for test_input in test_cases:
        result = parse_experience(test_input)
        print(f"\nInput: {test_input}")
        print(f"  General years: {result['experience_general_years']}")
        print(f"  Specific years: {result['experience_specific_years']}")
        print(f"  Public required: {result['sector_public_required']}")
        print(f"  Private required: {result['sector_private_required']}")
    
    print("\n" + "=" * 80)
    print("✓ Parser test complete")