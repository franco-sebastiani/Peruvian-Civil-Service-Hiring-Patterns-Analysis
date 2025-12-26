"""
Knowledge parser for processing phase.

Parses knowledge/skills requirements from comma or semicolon-separated text
into a structured list of individual skills.

Examples:
  "GESTIÓN PÚBLICA, LEGISLACIÓN LABORAL, LEY N° 30057" 
    → ["GESTIÓN PÚBLICA", "LEGISLACIÓN LABORAL", "LEY N° 30057"]
  
  "Excel; Word; PowerPoint"
    → ["EXCEL", "WORD", "POWERPOINT"]
  
  "Single skill only"
    → ["SINGLE SKILL ONLY"]
"""

import re
from servir.src.processing.parsers.text_parser import clean_text


def clean_knowledge(raw_knowledge):
    """
    Parse knowledge/skills text into a structured list.
    
    Process:
    1. Generic text cleaning (trim, punctuation)
    2. Detect separator (comma or semicolon)
    3. Split by separator
    4. Clean each individual skill
    5. Remove duplicates
    6. Sort alphabetically
    
    Args:
        raw_knowledge: Raw knowledge text from collection database
    
    Returns:
        list: List of cleaned skill strings, or None if empty/invalid
              Examples: ["SKILL 1", "SKILL 2", "SKILL 3"]
    """
    
    # Step 1: Generic text cleaning
    cleaned = clean_text(raw_knowledge)
    
    # Handle None or "NO INFO" results
    if not cleaned or cleaned == 'NO INFO':
        return None
    
    try:
        # Step 2: Detect separator and split
        # Check if semicolon is present (takes priority over comma)
        if ';' in cleaned:
            skills = cleaned.split(';')
        elif ',' in cleaned:
            skills = cleaned.split(',')
        else:
            # No separator found, treat entire string as single skill
            skills = [cleaned]
        
        # Step 3: Clean each individual skill
        cleaned_skills = []
        for skill in skills:
            # Trim whitespace
            skill = skill.strip()
            
            # Remove leading/trailing quotes
            skill = skill.strip('"\'')
            
            # Remove extra internal spaces
            skill = ' '.join(skill.split())
            
            # Skip empty items
            if skill and skill != 'NO INFO':
                cleaned_skills.append(skill)
        
        # Step 4: Remove duplicates while preserving order
        # Use dict.fromkeys() to maintain insertion order (Python 3.7+)
        cleaned_skills = list(dict.fromkeys(cleaned_skills))
        
        # Step 5: Sort alphabetically for consistency
        cleaned_skills.sort()
        
        # Handle empty result
        if not cleaned_skills:
            return None
        
        return cleaned_skills
    
    except Exception:
        return None


def format_knowledge_for_display(skills_list):
    """
    Format a knowledge list back to human-readable string.
    
    Useful for debugging or displaying parsed knowledge.
    
    Args:
        skills_list: List of skill strings
    
    Returns:
        str: Skills joined by ", "
    """
    if not skills_list:
        return "NO INFO"
    
    return ", ".join(skills_list)