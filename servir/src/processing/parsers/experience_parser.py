"""
Experience parser for processing phase.

Cleans experience requirements text field.
Just cleaning - no classification logic.

Examples:
  "  Mínimo 3 años de experiencia en gestión pública  "
    → "MÍNIMO 3 AÑOS DE EXPERIENCIA EN GESTIÓN PÚBLICA"
  
  "5 years  in  project management"
    → "5 YEARS IN PROJECT MANAGEMENT"
"""

from servir.src.processing.parsers.text_parser import clean_text


def clean_experience(raw_experience):
    """
    Clean experience requirements text field.
    
    Process:
    1. Generic text cleaning (trim, quotes, punctuation)
    2. Standardize spacing
    3. Return cleaned text
    
    Args:
        raw_experience: Raw experience text from collection database
    
    Returns:
        str: Cleaned experience text, or None if empty/invalid
    """
    
    # Step 1: Generic text cleaning
    cleaned = clean_text(raw_experience)
    
    # Handle None or "NO INFO" results
    if not cleaned or cleaned == 'NO INFO':
        return None
    
    try:
        # Step 2: Remove extra spaces
        cleaned = ' '.join(cleaned.split())
        
        return cleaned
    
    except Exception:
        return None