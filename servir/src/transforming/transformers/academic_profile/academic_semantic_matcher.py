"""
Semantic matching logic for academic profiles to CLASIFICADOR programs.

Uses sentence embeddings to match SERVIR academic profiles against 
CLASIFICADOR program names based on semantic meaning.
"""

import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


class AcademicSemanticMatcher:
    """Match academic profiles to CLASIFICADOR programs using semantic similarity."""
    
    def __init__(self, clasificador_programs_df, model_name='hiiamsid/sentence_similarity_spanish_es'):
        """
        Initialize matcher with CLASIFICADOR program catalog and semantic model.
        
        Args:
            clasificador_programs_df (DataFrame): CLASIFICADOR programs from database
            model_name (str): Name of sentence-transformer model
        """
        self.model = SentenceTransformer(model_name)
        self.programs = clasificador_programs_df
        self.program_names = None
        self.program_embeddings = None
        self._create_embeddings()
    
    def _create_embeddings(self):
        """Create embeddings for all CLASIFICADOR program names."""
        self.program_names = self.programs['programa_nombre'].tolist()
        
        print(f"Creating embeddings for {len(self.program_names)} CLASIFICADOR programs...")
        self.program_embeddings = self.model.encode(
            self.program_names,
            show_progress_bar=True
        )
        print(f"✓ Created embeddings for {len(self.program_names)} programs")
    
    def match_profile(self, academic_profile, search_levels=None, top_k=10, threshold=0):
        """
        Match a SERVIR academic profile to CLASIFICADOR programs.
        
        Args:
            academic_profile (str): Academic profile from SERVIR
            search_levels (list): Filter to these academic levels (e.g., [3,4,5] or [6])
            top_k (int): Number of top candidates to return
            threshold (float): Minimum cosine similarity (0-1)
        
        Returns:
            list of dicts with match information
        """
        if not academic_profile or not isinstance(academic_profile, str):
            return []
        
        # Filter programs by academic level if specified
        if search_levels:
            filtered_programs = self.programs[self.programs['academic_level'].isin(search_levels)]
            filtered_indices = filtered_programs.index.tolist()
            
            if len(filtered_indices) == 0:
                return []  # No programs at this level
            
            filtered_embeddings = self.program_embeddings[filtered_indices]
        else:
            filtered_programs = self.programs
            filtered_indices = list(range(len(self.programs)))
            filtered_embeddings = self.program_embeddings
        
        # Create embedding for academic profile
        profile_embedding = self.model.encode(academic_profile)
        
        # Calculate cosine similarity
        similarities = cosine_similarity([profile_embedding], filtered_embeddings)[0]
        
        # Convert to 0-100 scale
        scores = (similarities * 100).astype(int)
        
        # Create candidates with full program information
        candidates = []
        for i, idx in enumerate(filtered_indices):
            if scores[i] >= (threshold * 100):
                program_row = self.programs.iloc[idx]
                candidates.append({
                    'programa_codigo': program_row.get('programa_codigo'),
                    'programa_nombre': program_row.get('programa_nombre'),
                    'campo_amplio_codigo': program_row.get('campo_amplio_codigo'),
                    'campo_amplio_nombre': program_row.get('campo_amplio_nombre'),
                    'campo_especifico_codigo': program_row.get('campo_especifico_codigo'),
                    'campo_especifico_nombre': program_row.get('campo_especifico_nombre'),
                    'campo_detallado_codigo': program_row.get('campo_detallado_codigo'),
                    'campo_detallado_nombre': program_row.get('campo_detallado_nombre'),
                    'confidence': int(scores[i])
                })
        
        # Sort by confidence, take top_k
        candidates = sorted(candidates, key=lambda x: x['confidence'], reverse=True)[:top_k]
        
        # Add rank
        for i, match in enumerate(candidates, 1):
            match['rank'] = i
        
        return candidates