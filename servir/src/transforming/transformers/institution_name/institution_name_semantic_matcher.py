"""
Semantic matching logic for institution names.

Uses sentence embeddings to match SERVIR institution names against 
MEF institution names based on meaning.
"""

import pandas as pd
from sentence_transformers import SentenceTransformer
from sklearn.metrics.pairwise import cosine_similarity


class InstitutionSemanticMatcher:
    """Match SERVIR institutions to MEF institutions using semantic similarity."""
    
    def __init__(self, mef_institutions_df, model_name='hiiamsid/sentence_similarity_spanish_es'):
        """
        Initialize matcher with MEF institution catalog and semantic model.
        
        Args:
            mef_institutions_df (DataFrame): MEF institutions from API
            model_name (str): Name of sentence-transformer model
        """
        self.model = SentenceTransformer(model_name)
        self.mef_institutions = mef_institutions_df
        self.institution_names = None
        self.institution_embeddings = None
        self._create_embeddings()
    
    def _create_embeddings(self):
        """Create embeddings for all MEF institution names."""
        # Use EJECUTORA_NOMBRE for matching
        self.institution_names = self.mef_institutions['EJECUTORA_NOMBRE'].tolist()
        
        print(f"Creating embeddings for {len(self.institution_names)} MEF institutions...")
        self.institution_embeddings = self.model.encode(
            self.institution_names, 
            show_progress_bar=True
        )
        print(f"✓ Created embeddings for {len(self.institution_names)} institutions")
    
    def match_institution(self, servir_institution_name, top_k=3, threshold=0):
        """
        Match a single SERVIR institution to MEF institutions using semantic similarity.
        
        Args:
            servir_institution_name (str): Institution name from SERVIR
            top_k (int): Number of top candidates to return
            threshold (float): Minimum cosine similarity (0-1)
        
        Returns:
            list of dicts with match information including all MEF identifiers
        """
        if not servir_institution_name or not isinstance(servir_institution_name, str):
            return []
        
        servir_institution_name = servir_institution_name.strip()
        
        # Create embedding for SERVIR institution
        institution_embedding = self.model.encode(servir_institution_name)
        
        # Calculate cosine similarity against all MEF institutions
        similarities = cosine_similarity([institution_embedding], self.institution_embeddings)[0]
        
        # Convert to 0-100 scale
        scores = (similarities * 100).astype(int)
        
        # Create candidates with full MEF information
        candidates = []
        for idx in range(len(self.institution_names)):
            if scores[idx] >= (threshold * 100):
                mef_row = self.mef_institutions.iloc[idx]
                candidates.append({
                    'ejecutora': mef_row.get('EJECUTORA'),
                    'ejecutora_nombre': mef_row.get('EJECUTORA_NOMBRE'),
                    'nivel_gobierno': mef_row.get('NIVEL_GOBIERNO'),
                    'nivel_gobierno_nombre': mef_row.get('NIVEL_GOBIERNO_NOMBRE'),
                    'sector': mef_row.get('SECTOR'),
                    'sector_nombre': mef_row.get('SECTOR_NOMBRE'),
                    'pliego': mef_row.get('PLIEGO'),
                    'pliego_nombre': mef_row.get('PLIEGO_NOMBRE'),
                    'sec_ejec': mef_row.get('SEC_EJEC'),
                    'departamento_ejecutora': mef_row.get('DEPARTAMENTO_EJECUTORA'),
                    'departamento_ejecutora_nombre': mef_row.get('DEPARTAMENTO_EJECUTORA_NOMBRE'),
                    'provincia_ejecutora': mef_row.get('PROVINCIA_EJECUTORA'),
                    'provincia_ejecutora_nombre': mef_row.get('PROVINCIA_EJECUTORA_NOMBRE'),
                    'distrito_ejecutora': mef_row.get('DISTRITO_EJECUTORA'),
                    'distrito_ejecutora_nombre': mef_row.get('DISTRITO_EJECUTORA_NOMBRE'),
                    'confidence': int(scores[idx])
                })
        
        # Sort by confidence descending, take top_k
        candidates = sorted(candidates, key=lambda x: x['confidence'], reverse=True)[:top_k]
        
        # Add rank
        for i, match in enumerate(candidates, 1):
            match['rank'] = i
        
        return candidates