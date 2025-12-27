"""
Knowledge parser for processing phase.

Cleans knowledge/skills text field.
Just cleaning - no classification logic.

Examples:
  "GESTIÓN PÚBLICA, LEGISLACIÓN LABORAL, LEY N° 30057" 
    → "GESTIÓN PÚBLICA, LEGISLACIÓN LABORAL, LEY N° 30057"
  
  "  Excel;  Word;  PowerPoint  "
    → "EXCEL; WORD; POWERPOINT"
"""

from servir.src.cleaning.parsers.text_parser import clean_text


def clean_knowledge(raw_knowledge):
    """
    Clean knowledge/skills text field.
    
    Process:
    1. Generic text cleaning (trim, quotes, punctuation)
    2. Standardize separator spacing (after commas/semicolons)
    3. Return cleaned text
    
    Args:
        raw_knowledge: Raw knowledge text from collection database
    
    Returns:
        str: Cleaned knowledge text, or None if empty/invalid
    """
    
    # Step 1: Generic text cleaning
    cleaned = clean_text(raw_knowledge)
    
    # Handle None or "NO INFO" results
    if not cleaned or cleaned == 'NO INFO':
        return None
    
    try:
        # Step 2: Standardize spacing around separators
        # Add space after comma if missing: "skill1,skill2" → "skill1, skill2"
        cleaned = cleaned.replace(',', ', ')
        # Remove multiple spaces after comma: ",  " → ", "
        cleaned = ', '.join([s.strip() for s in cleaned.split(',')])
        
        # Same for semicolons: "skill1;skill2" → "skill1; skill2"
        cleaned = cleaned.replace(';', '; ')
        cleaned = '; '.join([s.strip() for s in cleaned.split(';')])
        
        # Remove extra spaces
        cleaned = ' '.join(cleaned.split())
        
        return cleaned
    
    except Exception:
        return None