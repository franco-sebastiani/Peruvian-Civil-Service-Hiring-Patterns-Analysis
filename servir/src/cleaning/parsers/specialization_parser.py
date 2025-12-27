"""
Specialization parser for processing phase.

Cleans specialization/major requirements text field.
Just cleaning - no classification logic.

Examples:
  "  Ingeniería, Administración, Contabilidad  "
    → "INGENIERÍA, ADMINISTRACIÓN, CONTABILIDAD"
  
  "Derecho;Derecho Administrativo;Derecho Público"
    → "DERECHO; DERECHO ADMINISTRATIVO; DERECHO PÚBLICO"
"""

from servir.src.cleaning.parsers.text_parser import clean_text


def clean_specialization(raw_specialization):
    """
    Clean specialization/major requirements text field.
    
    Process:
    1. Generic text cleaning (trim, quotes, punctuation)
    2. Standardize separator spacing (after commas/semicolons)
    3. Return cleaned text
    
    Args:
        raw_specialization: Raw specialization text from collection database
    
    Returns:
        str: Cleaned specialization text, or None if empty/invalid
    """
    
    # Step 1: Generic text cleaning
    cleaned = clean_text(raw_specialization)
    
    # Handle None or "NO INFO" results
    if not cleaned or cleaned == 'NO INFO':
        return None
    
    try:
        # Step 2: Standardize spacing around separators
        # Add space after comma if missing: "spec1,spec2" → "spec1, spec2"
        cleaned = cleaned.replace(',', ', ')
        # Remove multiple spaces after comma: ",  " → ", "
        cleaned = ', '.join([s.strip() for s in cleaned.split(',')])
        
        # Same for semicolons: "spec1;spec2" → "spec1; spec2"
        cleaned = cleaned.replace(';', '; ')
        cleaned = '; '.join([s.strip() for s in cleaned.split(';')])
        
        # Remove extra spaces
        cleaned = ' '.join(cleaned.split())
        
        return cleaned
    
    except Exception:
        return None