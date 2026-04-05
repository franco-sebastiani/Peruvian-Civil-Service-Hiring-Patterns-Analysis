"""
Fuzzy matching logic for academic profiles to CLASIFICADOR programs.

Uses token-based string similarity to match SERVIR academic profiles
against CLASIFICADOR program names.
"""

from thefuzz import fuzz
import pandas as pd


class AcademicFuzzyMatcher:
    """Match academic profiles to CLASIFICADOR programs using fuzzy string matching."""
    
    def __init__(self, clasificador_programs_df):
        """
        Initialize matcher with CLASIFICADOR program catalog.
        
        Args:
            clasificador_programs_df (DataFrame): CLASIFICADOR programs from database
        """
        self.programs = clasificador_programs_df
        print(f"Loaded {len(self.programs)} CLASIFICADOR programs (fuzzy matcher)")
    
    def match_profile(self, academic_profile, search_levels=None, top_k=10, threshold=0):
        """
        Fuzzy match a SERVIR academic profile to CLASIFICADOR programs.
        
        Args:
            academic_profile (str): Academic profile from SERVIR
            search_levels (list): Filter to these academic levels
            top_k (int): Number of top candidates to return
            threshold (int): Minimum confidence score (0-100)
        
        Returns:
            list of dicts with match information
        """
        if not academic_profile or not isinstance(academic_profile, str):
            return []
        
        academic_profile = academic_profile.strip().lower()
        
        # Filter programs by academic level if specified
        if search_levels:
            filtered_programs = self.programs[self.programs['academic_level'].isin(search_levels)]
        else:
            filtered_programs = self.programs
        
        if len(filtered_programs) == 0:
            return []
        
        # Score all programs
        scores = []
        for idx, row in filtered_programs.iterrows():
            program_name = str(row.get('programa_nombre', '')).lower()
            
            # Use token_set_ratio for better partial matching
            similarity = fuzz.token_set_ratio(academic_profile, program_name)
            
            if similarity >= threshold:
                scores.append({
                    'programa_codigo': row.get('programa_codigo'),
                    'programa_nombre': row.get('programa_nombre'),
                    'campo_amplio_codigo': row.get('campo_amplio_codigo'),
                    'campo_amplio_nombre': row.get('campo_amplio_nombre'),
                    'campo_especifico_codigo': row.get('campo_especifico_codigo'),
                    'campo_especifico_nombre': row.get('campo_especifico_nombre'),
                    'campo_detallado_codigo': row.get('campo_detallado_codigo'),
                    'campo_detallado_nombre': row.get('campo_detallado_nombre'),
                    'confidence': similarity
                })
        
        # Sort by confidence, take top_k
        scores = sorted(scores, key=lambda x: x['confidence'], reverse=True)[:top_k]
        
        # Add rank
        for i, match in enumerate(scores, 1):
            match['rank'] = i
        
        return scores