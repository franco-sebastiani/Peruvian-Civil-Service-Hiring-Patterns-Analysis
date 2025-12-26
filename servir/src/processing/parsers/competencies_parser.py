"""
Competencies parser for processing phase.

Parses competencies requirements from comma or semicolon-separated text
into a structured list of individual competencies.

Similar to knowledge_parser.py but specifically for competencies/behavioral skills.

Examples:
  "LIDERAZGO, COMUNICACIÓN, TRABAJO EN EQUIPO"
    → ["COMUNICACIÓN", "LIDERAZGO", "TRABAJO EN EQUIPO"]
  
  "Problem solving; Critical thinking; Adaptability"
    → ["ADAPTABILITY", "CRITICAL THINKING", "PROBLEM SOLVING"]
"""

import re
from servir.src.processing.parsers.text_parser import clean_text


def clean_competencies(raw_competencies):
    """
    Parse competencies text into a structured list.
    
    Process:
    1. Generic text cleaning (trim, punctuation)
    2. Detect separator (comma or semicolon)
    3. Split by separator
    4. Clean each individual competency
    5. Remove duplicates
    6. Sort alphabetically
    
    Args:
        raw_competencies: Raw competencies text from collection database
    
    Returns:
        list: List of cleaned competency strings, or None if empty/invalid
              Examples: ["COMPETENCY 1", "COMPETENCY 2", "COMPETENCY 3"]
    """
    
    # Step 1: Generic text cleaning
    cleaned = clean_text(raw_competencies)
    
    # Handle None or "NO INFO" results
    if not cleaned or cleaned == 'NO INFO':
        return None
    
    try:
        # Step 2: Detect separator and split
        # Check if semicolon is present (takes priority over comma)
        if ';' in cleaned:
            competencies = cleaned.split(';')
        elif ',' in cleaned:
            competencies = cleaned.split(',')
        else:
            # No separator found, treat entire string as single competency
            competencies = [cleaned]
        
        # Step 3: Clean each individual competency
        cleaned_competencies = []
        for competency in competencies:
            # Trim whitespace
            competency = competency.strip()
            
            # Remove leading/trailing quotes
            competency = competency.strip('"\'')
            
            # Remove extra internal spaces
            competency = ' '.join(competency.split())
            
            # Skip empty items
            if competency and competency != 'NO INFO':
                cleaned_competencies.append(competency)
        
        # Step 4: Remove duplicates while preserving order
        # Use dict.fromkeys() to maintain insertion order (Python 3.7+)
        cleaned_competencies = list(dict.fromkeys(cleaned_competencies))
        
        # Step 5: Sort alphabetically for consistency
        cleaned_competencies.sort()
        
        # Handle empty result
        if not cleaned_competencies:
            return None
        
        return cleaned_competencies
    
    except Exception:
        return None


def format_competencies_for_display(competencies_list):
    """
    Format a competencies list back to human-readable string.
    
    Useful for debugging or displaying parsed competencies.
    
    Args:
        competencies_list: List of competency strings
    
    Returns:
        str: Competencies joined by ", "
    """
    if not competencies_list:
        return "NO INFO"
    
    return ", ".join(competencies_list)