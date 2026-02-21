"""
Experience Embedder - Converts experience requirements to vector representations.

Creates semantic embeddings that capture the full meaning and context of 
experience requirements for clustering analysis.

Optional: Apply PCA to reduce dimensions (384 → 50) for efficiency.
"""

from sentence_transformers import SentenceTransformer
from sklearn.decomposition import PCA
import numpy as np


class ExperienceEmbedder:
    """Embed experience requirements using sentence transformers."""
    
    def __init__(self, model_name='hiiamsid/sentence_similarity_spanish_es', use_pca=False, n_components=50):
        """
        Initialize embedder with sentence transformer model.
        
        Args:
            model_name (str): Sentence transformer model to use
            use_pca (bool): Whether to apply PCA dimension reduction
            n_components (int): Target dimensions if use_pca=True (default: 50)
        """
        print(f"Loading sentence transformer model: {model_name}")
        self.model = SentenceTransformer(model_name)
        self.use_pca = use_pca
        self.n_components = n_components
        self.pca = None
        
        if use_pca:
            print(f"PCA enabled: Will reduce from 384 to {n_components} dimensions")
    
    def embed_single(self, text):
        """
        Embed a single experience requirement text.
        
        Args:
            text (str): Experience requirement text
        
        Returns:
            numpy array: Embedding vector (384-dim or n_components if PCA)
        """
        if not text or not isinstance(text, str):
            # Return zero vector for null/empty
            dim = self.n_components if self.use_pca else 384
            return np.zeros(dim)
        
        # Create embedding
        embedding = self.model.encode(text)
        
        # Apply PCA if enabled and fitted
        if self.use_pca and self.pca is not None:
            embedding = self.pca.transform([embedding])[0]
        
        return embedding
    
    def embed_batch(self, texts, fit_pca=True):
        """
        Embed multiple experience requirements.
        
        Args:
            texts (list): List of experience requirement strings
            fit_pca (bool): If True and use_pca=True, fit PCA on this batch
        
        Returns:
            numpy array: Matrix of embeddings (n_texts × embedding_dim)
        """
        print(f"Embedding {len(texts)} experience requirements...")
        
        # Handle None/empty texts
        clean_texts = [text if text else "" for text in texts]
        
        # Create embeddings
        embeddings = self.model.encode(clean_texts, show_progress_bar=True)
        
        # Apply PCA if requested
        if self.use_pca:
            if fit_pca:
                print(f"Fitting PCA: {embeddings.shape[1]} → {self.n_components} dimensions")
                self.pca = PCA(n_components=self.n_components)
                embeddings = self.pca.fit_transform(embeddings)
                print(f"Explained variance: {self.pca.explained_variance_ratio_.sum():.2%}")
            elif self.pca is not None:
                embeddings = self.pca.transform(embeddings)
            else:
                raise ValueError("PCA requested but not fitted. Set fit_pca=True first.")
        
        return embeddings
    
    def get_embedding_dim(self):
        """
        Get the dimensionality of embeddings produced.
        
        Returns:
            int: Number of dimensions (384 or n_components if PCA)
        """
        return self.n_components if self.use_pca else 384


# Testing
if __name__ == "__main__":
    print("Testing Experience Embedder")
    print("=" * 80)
    
    # Test data
    test_texts = [
        "EXPERIENCIA GENERAL DE 03 AÑOS EN SECTOR PÚBLICO",
        "3 AÑOS EXPERIENCIA EN SECTOR PÚBLICO",  # Similar meaning
        "10 AÑOS EXPERIENCIA EN SECTOR PRIVADO",  # Different
        None,  # Test null handling
    ]
    
    # Test without PCA
    print("\nTest 1: Embeddings without PCA")
    embedder = ExperienceEmbedder(use_pca=False)
    
    for text in test_texts[:3]:
        embedding = embedder.embed_single(text)
        print(f"\nText: {text}")
        print(f"  Embedding shape: {embedding.shape}")
        print(f"  First 5 values: {embedding[:5]}")
    
    # Test with PCA
    print("\n" + "=" * 80)
    print("Test 2: Embeddings with PCA (50 dimensions)")
    embedder_pca = ExperienceEmbedder(use_pca=True, n_components=50)
    
    batch_embeddings = embedder_pca.embed_batch(test_texts, fit_pca=True)
    print(f"\nBatch embedding shape: {batch_embeddings.shape}")
    print(f"Expected: ({len(test_texts)}, 50)")
    
    print("\n" + "=" * 80)
    print("✓ Embedder test complete")