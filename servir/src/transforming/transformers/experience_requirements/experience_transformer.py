"""
Experience Transformer - Orchestrates parsing and embedding of experience requirements.

Hybrid approach:
1. Parse structured fields (years, sector) using regex
2. Create embeddings for semantic clustering
3. Combine both for comprehensive transformation

Output: Structured fields + embedding vectors for analysis phase.
"""

import sys
from pathlib import Path
import pandas as pd
import numpy as np
from tqdm import tqdm

# Add parent directory to path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from experience_parser import parse_experience
from experience_embedder import ExperienceEmbedder


class ExperienceTransformer:
    """Transform experience requirements using hybrid parsing + embedding approach."""
    
    def __init__(self, use_pca=False, n_components=50):
        """
        Initialize transformer.
        
        Args:
            use_pca (bool): Whether to reduce embedding dimensions
            n_components (int): Target dimensions if use_pca=True
        """
        self.use_pca = use_pca
        self.n_components = n_components
        self.embedder = None
    
    def transform_single(self, experience_text):
        """
        Transform a single experience requirement.
        
        Args:
            experience_text (str): Experience requirement text
        
        Returns:
            dict: Structured fields + embedding
        """
        # Parse structured fields
        parsed = parse_experience(experience_text)
        
        # Create embedding (lazy load embedder)
        if self.embedder is None:
            self.embedder = ExperienceEmbedder(use_pca=self.use_pca, n_components=self.n_components)
        
        embedding = self.embedder.embed_single(experience_text)
        
        # Combine results
        result = {
            **parsed,
            'experience_requirements_raw': experience_text,
            'experience_embedding': embedding  # numpy array
        }
        
        return result
    
    def transform_batch(self, experience_texts, fit_pca=True):
        """
        Transform multiple experience requirements.
        
        Args:
            experience_texts (list): List of experience requirement strings
            fit_pca (bool): If using PCA, fit on this batch
        
        Returns:
            pandas DataFrame: Transformed data with structured fields + embeddings
        """
        print(f"Transforming {len(experience_texts)} experience requirements...")
        
        # Parse all texts
        print("Parsing structured fields...")
        parsed_results = []
        for text in tqdm(experience_texts, desc="Parsing"):
            parsed = parse_experience(text)
            parsed['experience_requirements_raw'] = text
            parsed_results.append(parsed)
        
        parsed_df = pd.DataFrame(parsed_results)
        
        # Create embeddings in batch (more efficient)
        print("Creating embeddings...")
        if self.embedder is None:
            self.embedder = ExperienceEmbedder(use_pca=self.use_pca, n_components=self.n_components)
        
        embeddings = self.embedder.embed_batch(experience_texts, fit_pca=fit_pca)
        
        # Convert embeddings to columns
        embedding_dim = self.embedder.get_embedding_dim()
        embedding_cols = {f'experience_embedding_{i}': embeddings[:, i] 
                         for i in range(embedding_dim)}
        
        embedding_df = pd.DataFrame(embedding_cols)
        
        # Combine parsed + embeddings
        result_df = pd.concat([parsed_df, embedding_df], axis=1)
        
        print(f"✓ Transformation complete!")
        print(f"  Structured fields: 4 columns")
        print(f"  Embedding dimensions: {embedding_dim} columns")
        print(f"  Total output columns: {len(result_df.columns)}")
        
        return result_df


# Testing
if __name__ == "__main__":
    print("Testing Experience Transformer")
    print("=" * 80)
    
    test_data = [
        "EXPERIENCIA GENERAL: TRES (03) AÑOS EXPERIENCIA ESPECÍFICA: DOS AÑOS EN SECTOR PÚBLICO",
        "EXPERIENCIA LABORAL GENERAL DE 01 AÑO EN EL SECTOR PÚBLICO Y/O PRIVADO",
        "SIN EXPERIENCIA",
        "EXPERIENCIA MINIMA DE 06 MESES EN EL SECTOR PUBLICO Y/O PRIVADO",
    ]
    
    # Test without PCA
    print("\nTest 1: Transform batch WITHOUT PCA")
    transformer = ExperienceTransformer(use_pca=False)
    result_df = transformer.transform_batch(test_data, fit_pca=False)
    
    print("\nSample output:")
    print(result_df[['experience_general_years', 'experience_specific_years', 
                     'sector_public_required', 'sector_private_required']].to_string())
    
    # Test with PCA
    print("\n" + "=" * 80)
    print("Test 2: Transform batch WITH PCA (50 dimensions)")
    transformer_pca = ExperienceTransformer(use_pca=True, n_components=50)
    result_df_pca = transformer_pca.transform_batch(test_data, fit_pca=True)
    
    print(f"\nOutput shape: {result_df_pca.shape}")
    print(f"Columns: {list(result_df_pca.columns[:10])}...")  # Show first 10
    
    print("\n" + "=" * 80)
    print("✓ Transformer test complete")