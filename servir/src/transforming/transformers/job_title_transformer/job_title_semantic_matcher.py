"""
Semantic matching logic for standardizing job titles to ISCO-08 categories.

Uses sentence embeddings to match job titles against ISCO descriptions based on meaning,
not just word overlap. This handles semantic relationships (e.g., "abogacía" ≈ "abogado").
"""

import sqlite3
from pathlib import Path
import torch
import torch.nn as nn
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity
import pandas as pd


class JobTitleSemanticMatcher:
    """Match job titles to ISCO-08 occupational categories using semantic similarity."""
    
    def __init__(self, isco_db_path, model_name='hiiamsid/sentence_similarity_spanish_es'):
        """
        Initialize matcher with ISCO-08 reference database and semantic model.
        
        Args:
            isco_db_path (str/Path): Path to isco_08_peru.db
            model_name (str): Name of sentence-transformer model to use
                Default: 'paraphrase-multilingual-MiniLM-L12-v2' (works with Spanish)
        """
        self.isco_db_path = Path(isco_db_path)
        self.model = SentenceTransformer(model_name)
        self.isco_descriptions = None
        self.isco_embeddings = None
        self._load_isco_descriptions()
    
    def _load_isco_descriptions(self):
        """Load all ISCO-08 level-4 descriptions and create embeddings."""
        conn = sqlite3.connect(self.isco_db_path)
        conn.row_factory = sqlite3.Row
        
        query = """
        SELECT codigo, descripcion 
        FROM grupos_primarios
        ORDER BY codigo
        """
        
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        # Store codes and descriptions
        self.isco_descriptions = dict(zip(df['codigo'], df['descripcion']))
        
        # Create embeddings for all descriptions (computed once at initialization)
        print(f"Creating embeddings for {len(self.isco_descriptions)} ISCO-08 descriptions...")
        descriptions_list = list(self.isco_descriptions.values())
        self.isco_embeddings = self.model.encode(descriptions_list, show_progress_bar=True)
        print(f"✓ Loaded {len(self.isco_descriptions)} ISCO-08 nivel 4 descriptions with embeddings")
    
    def match_title(self, job_title, top_k=3, threshold=0):
        """
        Match a single job title to ISCO categories using semantic similarity.
        
        Args:
            job_title (str): Cleaned job title from SERVIR
            top_k (int): Number of top candidates to return (default: 3)
            threshold (float): Minimum cosine similarity to include (0-1, default: 0)
        
        Returns:
            list of dicts: Each dict has keys:
                - codigo: ISCO level-4 code
                - descripcion: ISCO description
                - confidence: Cosine similarity score (0-100 scale)
                - rank: Position in ranked results
        
        Example:
            >>> matcher.match_title("Abogado especialista en derecho laboral")
            [
                {'codigo': '2614', 'descripcion': 'Abogados', 'confidence': 92, 'rank': 1},
                {'codigo': '2611', 'descripcion': '...', 'confidence': 85, 'rank': 2},
                ...
            ]
        """
        if not job_title or not isinstance(job_title, str):
            return []
        
        job_title = job_title.strip()
        
        # Create embedding for this job title
        title_embedding = self.model.encode(job_title)
        
        # Calculate cosine similarity against all ISCO descriptions
        # Result shape: (1, num_isco_descriptions)
        similarities = cosine_similarity([title_embedding], self.isco_embeddings)[0]
        
        # Convert to 0-100 scale for readability
        scores = (similarities * 100).astype(int)
        
        # Pair with ISCO codes and sort by similarity
        isco_codes = list(self.isco_descriptions.keys())
        candidates = [
            {
                'codigo': isco_codes[idx],
                'descripcion': self.isco_descriptions[isco_codes[idx]],
                'confidence': int(scores[idx])
            }
            for idx in range(len(isco_codes))
            if scores[idx] >= (threshold * 100)
        ]
        
        # Sort by confidence descending, take top_k
        candidates = sorted(candidates, key=lambda x: x['confidence'], reverse=True)[:top_k]
        
        # Add rank
        for i, match in enumerate(candidates, 1):
            match['rank'] = i
        
        return candidates
    
    def match_batch(self, job_titles_df, top_k=3, threshold=0):
        """
        Match multiple job titles using semantic similarity.
        
        Args:
            job_titles_df (DataFrame): Must have 'job_title' column
            top_k (int): Number of top candidates per title
            threshold (float): Minimum cosine similarity (0-1 scale)
        
        Returns:
            DataFrame with columns:
                - job_title: Original title
                - candidate_codigo: ISCO code
                - candidate_descripcion: ISCO description
                - confidence: Confidence score (0-100)
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


def create_validation_db(matches_df, output_db_path):
    """
    Create job_title_validation.db with semantic match results.
    
    Args:
        matches_df (DataFrame): Output from match_batch()
        output_db_path (str/Path): Where to save the DB
    """
    output_db_path = Path(output_db_path)
    output_db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(output_db_path)
    
    # Create fuzzy_matches table
    conn.execute("""
    CREATE TABLE IF NOT EXISTS fuzzy_matches (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        job_title TEXT NOT NULL,
        candidate_codigo TEXT NOT NULL,
        candidate_descripcion TEXT NOT NULL,
        confidence INTEGER NOT NULL,
        rank INTEGER NOT NULL,
        validated INTEGER DEFAULT 0,
        validated_codigo TEXT,
        validated_descripcion TEXT,
        notes TEXT,
        UNIQUE(job_title, candidate_codigo, rank)
    )
    """)
    
    # Insert matches
    for _, row in matches_df.iterrows():
        conn.execute("""
        INSERT OR IGNORE INTO fuzzy_matches 
        (job_title, candidate_codigo, candidate_descripcion, confidence, rank)
        VALUES (?, ?, ?, ?, ?)
        """, (
            row['job_title'],
            row['candidate_codigo'],
            row['candidate_descripcion'],
            row['confidence'],
            row['rank']
        ))
    
    conn.commit()
    conn.close()
    print(f"Validation DB created: {output_db_path}")