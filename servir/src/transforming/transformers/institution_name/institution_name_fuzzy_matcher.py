"""
Fuzzy matching logic for institution names.

Uses token-based string similarity to match SERVIR institution names
against MEF institution names. Good for exact/near-exact word matches.
"""

from thefuzz import fuzz
import pandas as pd


class InstitutionFuzzyMatcher:
    """Match SERVIR institutions to MEF institutions using fuzzy string matching."""
    
    def __init__(self, mef_institutions_df):
        """
        Initialize matcher with MEF institution catalog.
        
        Args:
            mef_institutions_df (DataFrame): MEF institutions from API
        """
        self.mef_institutions = mef_institutions_df
        print(f"Loaded {len(self.mef_institutions)} MEF institutions (fuzzy matcher)")
    
    def match_institution(self, servir_institution_name, top_k=3, threshold=0):
        """
        Fuzzy match a SERVIR institution against all MEF institutions.
        Uses token_set_ratio for word-order-independent matching.
        
        Args:
            servir_institution_name (str): Institution name from SERVIR
            top_k (int): Number of top candidates to return
            threshold (int): Minimum confidence score (0-100)
        
        Returns:
            list of dicts with match information including all MEF identifiers
        """
        if not servir_institution_name or not isinstance(servir_institution_name, str):
            return []
        
        servir_institution_name = servir_institution_name.strip().lower()
        
        # Score all MEF institutions against this SERVIR institution
        scores = []
        for idx, row in self.mef_institutions.iterrows():
            mef_name = str(row.get('EJECUTORA_NOMBRE', '')).lower()
            
            # Use token_set_ratio for better partial matching
            similarity = fuzz.token_set_ratio(servir_institution_name, mef_name)
            
            if similarity >= threshold:
                scores.append({
                    'ejecutora': row.get('EJECUTORA'),
                    'ejecutora_nombre': row.get('EJECUTORA_NOMBRE'),
                    'nivel_gobierno': row.get('NIVEL_GOBIERNO'),
                    'nivel_gobierno_nombre': row.get('NIVEL_GOBIERNO_NOMBRE'),
                    'sector': row.get('SECTOR'),
                    'sector_nombre': row.get('SECTOR_NOMBRE'),
                    'pliego': row.get('PLIEGO'),
                    'pliego_nombre': row.get('PLIEGO_NOMBRE'),
                    'sec_ejec': row.get('SEC_EJEC'),
                    'departamento_ejecutora': row.get('DEPARTAMENTO_EJECUTORA'),
                    'departamento_ejecutora_nombre': row.get('DEPARTAMENTO_EJECUTORA_NOMBRE'),
                    'provincia_ejecutora': row.get('PROVINCIA_EJECUTORA'),
                    'provincia_ejecutora_nombre': row.get('PROVINCIA_EJECUTORA_NOMBRE'),
                    'distrito_ejecutora': row.get('DISTRITO_EJECUTORA'),
                    'distrito_ejecutora_nombre': row.get('DISTRITO_EJECUTORA_NOMBRE'),
                    'confidence': similarity
                })
        
        # Sort by confidence descending, take top_k
        scores = sorted(scores, key=lambda x: x['confidence'], reverse=True)[:top_k]
        
        # Add rank
        for i, match in enumerate(scores, 1):
            match['rank'] = i
        
        return scores