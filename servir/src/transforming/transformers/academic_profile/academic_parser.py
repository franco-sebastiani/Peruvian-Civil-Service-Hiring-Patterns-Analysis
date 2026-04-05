"""
Academic Parser - Extracts academic level and additional requirements.

Uses pattern matching to extract:
- Academic level (secundaria, técnico, bachiller, título, maestría, etc.)
- Additional requirement flags (accepts_related, requires_licensing, etc.)
"""

import json
from pathlib import Path


# Load patterns from JSON
_patterns_path = Path(__file__).parent / "academic_levels.json"
with open(_patterns_path, 'r', encoding='utf-8') as f:
    PATTERNS = json.load(f)


def extract_academic_level(text):
    """
    Extract academic level information from academic profile text.
    
    Args:
        text (str): Academic profile text
    
    Returns:
        dict: {
            'search_levels': list of int (which CLASIFICADOR levels to search),
            'level_name': str,
            'thesis_required': 0 or 1
        } or None
    """
    if not text or not isinstance(text, str):
        return None
    
    text_upper = text.upper()
    
    # Try to match level patterns (order matters - check more specific first)
    level_patterns_ordered = [
        'TITULO PROFESIONAL',
        'TITULO UNIVERSITARIO',
        'PROFESIONAL TECNICO',
        'AUXILIAR TECNICO',
        'TITULO TECNICO',
        'SECUNDARIA COMPLETA',
        'LICENCIADO',
        'INGENIERO',
        'BACHILLER',
        'EGRESADO',
        'TITULADO',
        'TECNICO',
        'MAESTRIA',
        'DOCTORADO'
    ]
    
    for pattern in level_patterns_ordered:
        if pattern in text_upper:
            level_info = PATTERNS['level_patterns'][pattern]
            return {
                'search_levels': level_info['search_levels'],
                'level_name': level_info['level_name'],
                'thesis_required': level_info['thesis_required']
            }
    
    return None


def extract_additional_requirements(text):
    """
    Extract additional requirement flags from text.
    
    Args:
        text (str): Academic profile text
    
    Returns:
        dict: Flags for various requirements (0/1)
    """
    if not text or not isinstance(text, str):
        return {
            'accepts_related_fields': 0,
            'requires_colegiado': 0,
            'requires_habilitado': 0,
            'multiple_options_allowed': 0
        }
    
    text_upper = text.upper()
    
    # Check for "accepts related fields"
    accepts_related = 0
    for pattern in PATTERNS['additional_requirement_patterns']['accepts_related_fields']:
        if pattern in text_upper:
            accepts_related = 1
            break
    
    # Check for "colegiado"
    requires_colegiado = 0
    for pattern in PATTERNS['additional_requirement_patterns']['requires_colegiado']:
        if pattern in text_upper:
            requires_colegiado = 1
            break
    
    # Check for "habilitado"
    requires_habilitado = 0
    for pattern in PATTERNS['additional_requirement_patterns']['requires_habilitado']:
        if pattern in text_upper:
            requires_habilitado = 1
            break
    
    # Check for multiple options
    multiple_options = 0
    for pattern in PATTERNS['additional_requirement_patterns']['multiple_options']:
        if pattern in text_upper:
            multiple_options = 1
            break
    
    return {
        'accepts_related_fields': accepts_related,
        'requires_colegiado': requires_colegiado,
        'requires_habilitado': requires_habilitado,
        'multiple_options_allowed': multiple_options
    }


def parse_academic_profile(text):
    """
    Parse academic profile text into structured fields.
    
    Args:
        text (str): Academic profile text
    
    Returns:
        dict: Extracted level and requirement flags
    """
    if not text or not isinstance(text, str):
        return {
            'search_levels': [],
            'level_name': None,
            'thesis_required': 0,
            'accepts_related_fields': 0,
            'requires_colegiado': 0,
            'requires_habilitado': 0,
            'multiple_options_allowed': 0
        }
    
    # Extract level
    level_info = extract_academic_level(text)
    
    # Extract additional requirements
    requirements = extract_additional_requirements(text)
    
    # Combine results
    return {
        'search_levels': level_info['search_levels'] if level_info else [],
        'level_name': level_info['level_name'] if level_info else None,
        'thesis_required': level_info['thesis_required'] if level_info else 0,
        **requirements
    }


# Testing
if __name__ == "__main__":
    print("Testing Academic Parser")
    print("=" * 80)
    
    test_cases = [
        "TITULO PROFESIONAL DE ABOGADO COLEGIADO Y HABILITADO",
        "BACHILLER EN ADMINISTRACION, ECONOMIA O CONTABILIDAD",
        "TITULO TECNICO EN SECRETARIADO EJECUTIVO",
        "SECUNDARIA COMPLETA",
        "EGRESADO EN DERECHO O AFINES",
        "LICENCIADO EN ENFERMERIA",
        "TITULO UNIVERSITARIO EN ADMINISTRACIÓN O ECONOMÍA Y/O AFINES",
        None
    ]
    
    for test_input in test_cases:
        result = parse_academic_profile(test_input)
        print(f"\nInput: {test_input}")
        print(f"  Level: {result['academic_level_name']} (code={result['academic_level_code']})")
        print(f"  Accepts related: {result['accepts_related_fields']}")
        print(f"  Requires colegiado: {result['requires_colegiado']}")
        print(f"  Requires habilitado: {result['requires_habilitado']}")
        print(f"  Multiple options: {result['multiple_options_allowed']}")
    
    print("\n" + "=" * 80)
    print("✓ Parser test complete")