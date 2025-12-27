"""
Competencies parser for processing phase.

Cleans competencies/behavioral skills text field.
Just cleaning - no classification logic.

Examples:
  "  LIDERAZGO,  COMUNICACIÓN,  TRABAJO EN EQUIPO  "
    → "LIDERAZGO, COMUNICACIÓN, TRABAJO EN EQUIPO"
  
  "Problem solving;Critical thinking;Adaptability"
    → "PROBLEM SOLVING; CRITICAL THINKING; ADAPTABILITY"
"""

from servir.src.processing.parsers.text_parser import clean_text


def clean_competencies(raw_competencies):
    """
    Clean competencies/behavioral skills text field.
    
    Process:
    1. Generic text cleaning (trim, quotes, punctuation)
    2. Standardize separator spacing (after commas/semicolons)
    3. Return cleaned text
    
    Args:
        raw_competencies: Raw competencies text from collection database
    
    Returns:
        str: Cleaned competencies text, or None if empty/invalid
    """
    
    # Step 1: Generic text cleaning
    cleaned = clean_text(raw_competencies)
    
    # Handle None or "NO INFO" results
    if not cleaned or cleaned == 'NO INFO':
        return None
    
    try:
        # Step 2: Standardize spacing around separators
        # Add space after comma if missing: "comp1,comp2" → "comp1, comp2"
        cleaned = cleaned.replace(',', ', ')
        # Remove multiple spaces after comma: ",  " → ", "
        cleaned = ', '.join([s.strip() for s in cleaned.split(',')])
        
        # Same for semicolons: "comp1;comp2" → "comp1; comp2"
        cleaned = cleaned.replace(';', '; ')
        cleaned = '; '.join([s.strip() for s in cleaned.split(';')])
        
        # Remove extra spaces
        cleaned = ' '.join(cleaned.split())
        
        return cleaned
    
    except Exception:
        return None