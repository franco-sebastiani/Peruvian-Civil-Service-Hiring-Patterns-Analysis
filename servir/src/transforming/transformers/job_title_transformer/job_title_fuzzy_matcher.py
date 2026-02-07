"""
Fuzzy matching logic for standardizing job titles to ISCO-08 categories.

Uses token-based string similarity (thefuzz library) to match job titles 
against ISCO descriptions. Good for exact/near-exact word matches.
Complements semantic matching for hybrid approach.
"""

import sqlite3
from pathlib import Path
from thefuzz import fuzz
import pandas as pd


class JobTitleFuzzyMatcher:
    """Match job titles to ISCO-08 occupational categories using fuzzy string matching."""
    
    def __init__(self, isco_db_path):
        """
        Initialize matcher with ISCO-08 reference database.
        
        Args:
            isco_db_path (str/Path): Path to isco_08_peru.db
        """
        self.isco_db_path = Path(isco_db_path)
        self.isco_descriptions = None
        self._load_isco_descriptions()
    
    def _load_isco_descriptions(self):
        """Load all ISCO-08 level-4 descriptions into memory for matching."""
        conn = sqlite3.connect(self.isco_db_path)
        conn.row_factory = sqlite3.Row
        
        query = """
        SELECT codigo, descripcion 
        FROM grupos_primarios
        ORDER BY codigo
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Store as dict for fast lookup: {codigo: descripcion}
        self.isco_descriptions = dict(zip(df['codigo'], df['descripcion']))
        print(f"Loaded {len(self.isco_descriptions)} ISCO-08 nivel 4 descriptions (fuzzy matcher)")
    
    def match_title(self, job_title, top_k=3, threshold=0):
        """
        Fuzzy match a single job title against all ISCO descriptions.
        Uses token_set_ratio for word-order-independent matching.
        
        Args:
            job_title (str): Cleaned job title from SERVIR
            top_k (int): Number of top candidates to return (default: 3)
            threshold (int): Minimum confidence score to include (0-100, default: 0)
        
        Returns:
            list of dicts: Each dict has keys:
                - codigo: ISCO level-4 code
                - descripcion: ISCO description
                - confidence: Similarity score (0-100)
                - rank: Position in ranked results
        
        Example:
            >>> matcher.match_title("Contador")
            [
                {'codigo': '2411', 'descripcion': 'Contadores', 'confidence': 95, 'rank': 1},
                {'codigo': '3313', 'descripcion': 'Técnicos en contabilidad', 'confidence': 72, 'rank': 2},
                ...
            ]
        """
        if not job_title or not isinstance(job_title, str):
            return []
        
        job_title = job_title.strip().lower()
        
        # Score all ISCO descriptions against this title
        scores = []
        for isco_code, isco_desc in self.isco_descriptions.items():
            # Use token_set_ratio for better partial matching
            # Handles word order variations and partial matches
            similarity = fuzz.token_set_ratio(job_title, isco_desc.lower())
            
            if similarity >= threshold:
                scores.append({
                    'codigo': isco_code,
                    'descripcion': isco_desc,
                    'confidence': similarity
                })
        
        # Sort by confidence descending, take top_k
        scores = sorted(scores, key=lambda x: x['confidence'], reverse=True)[:top_k]
        
        # Add rank
        for i, match in enumerate(scores, 1):
            match['rank'] = i
        
        return scores
    
    def match_batch(self, job_titles_df, top_k=3, threshold=0):
        """
        Fuzzy match multiple job titles.
        
        Args:
            job_titles_df (DataFrame): Must have 'job_title' column
            top_k (int): Number of top candidates per title
            threshold (int): Minimum confidence score
        
        Returns:
            DataFrame with columns:
                - job_title: Original title
                - candidate_codigo: Top ISCO code
                - candidate_descripcion: Top ISCO description
                - confidence: Confidence score
                - rank: Position (1, 2, 3...)
        
        Example:
            >>> results = matcher.match_batch(cleaned_df)
            >>> results[results['rank'] == 1]  # Get best matches only
        """
        results = []
        
        for idx, row in job_titles_df.iterrows():
            job_title = row['job_title']
            matches = self.match_title(job_title, top_k=top_k, threshold=threshold)
            
            for match in matches:
                results.append({
                    'job_title': job_title,
                    'candidate_codigo': match['codigo'],
                    'candidate_descripcion': match['descripcion'],
                    'confidence': match['confidence'],
                    'rank': match['rank']
                })
        
        return pd.DataFrame(results)