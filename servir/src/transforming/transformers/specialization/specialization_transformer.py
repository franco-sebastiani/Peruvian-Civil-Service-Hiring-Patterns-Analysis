"""
Specialization Transformer - Transforms specialization requirements using LDA topic modeling.

Discovers latent topics in specialization requirements and represents each requirement
as a distribution over topics.

Output: Topic probabilities (15 topics) for clustering and analysis.
"""

from sklearn.decomposition import LatentDirichletAllocation
from sklearn.feature_extraction.text import CountVectorizer
import pandas as pd
import numpy as np
import pickle
from pathlib import Path


class SpecializationTransformer:
    """Transform specialization requirements using LDA topic modeling."""
    
    def __init__(self, n_topics=15, max_features=1000):
        """
        Initialize transformer with LDA parameters.
        
        Args:
            n_topics (int): Number of latent topics to discover
            max_features (int): Maximum vocabulary size
        """
        self.n_topics = n_topics
        self.max_features = max_features
        self.vectorizer = None
        self.lda_model = None
    
    def fit_transform(self, specialization_texts):
        """
        Fit LDA model and transform specialization requirements.
        
        Args:
            specialization_texts (list): List of specialization requirement strings
        
        Returns:
            pandas DataFrame: Topic distributions (n_texts × n_topics)
        """
        print(f"Fitting LDA model with {self.n_topics} topics...")
        
        # Handle None/empty texts and special values
        clean_texts = []
        for text in specialization_texts:
            if not text or not isinstance(text, str):
                clean_texts.append("")
            elif text.upper() in ["NO APLICA", "NO REQUIERE", "NO INFO"]:
                clean_texts.append("")  # Treat as empty
            else:
                clean_texts.append(text)
        
        # Create document-term matrix
        print("Creating document-term matrix...")
        self.vectorizer = CountVectorizer(
            max_features=self.max_features,
            min_df=2,  # Ignore very rare words
            max_df=0.8  # Ignore very common words (acts like stopword removal)
        )
        
        doc_term_matrix = self.vectorizer.fit_transform(clean_texts)
        print(f"  Vocabulary size: {len(self.vectorizer.get_feature_names_out())}")
        print(f"  Document-term matrix shape: {doc_term_matrix.shape}")
        
        # Fit LDA
        print(f"Fitting LDA model ({self.n_topics} topics)...")
        self.lda_model = LatentDirichletAllocation(
            n_components=self.n_topics,
            random_state=42,
            max_iter=20,
            learning_method='batch'
        )
        
        topic_distributions = self.lda_model.fit_transform(doc_term_matrix)
        
        print(f"✓ LDA model fitted")
        print(f"  Perplexity: {self.lda_model.perplexity(doc_term_matrix):.2f}")
        
        # Convert to DataFrame with column names
        topic_cols = {f'specialization_topic_{i}': topic_distributions[:, i] 
                     for i in range(self.n_topics)}
        
        result_df = pd.DataFrame(topic_cols)
        
        return result_df
    
    def transform(self, specialization_texts):
        """
        Transform new specialization requirements using fitted model.
        
        Args:
            specialization_texts (list): List of specialization requirement strings
        
        Returns:
            pandas DataFrame: Topic distributions
        """
        if self.vectorizer is None or self.lda_model is None:
            raise ValueError("Model not fitted. Call fit_transform() first.")
        
        # Handle None/empty and special values
        clean_texts = []
        for text in specialization_texts:
            if not text or not isinstance(text, str):
                clean_texts.append("")
            elif text.upper() in ["NO APLICA", "NO REQUIERE", "NO INFO"]:
                clean_texts.append("")
            else:
                clean_texts.append(text)
        
        # Transform using fitted vectorizer and model
        doc_term_matrix = self.vectorizer.transform(clean_texts)
        topic_distributions = self.lda_model.transform(doc_term_matrix)
        
        # Convert to DataFrame
        topic_cols = {f'specialization_topic_{i}': topic_distributions[:, i] 
                     for i in range(self.n_topics)}
        
        return pd.DataFrame(topic_cols)
    
    def get_top_words_per_topic(self, n_words=10):
        """
        Get top words for each topic (for interpretation).
        
        Args:
            n_words (int): Number of top words per topic
        
        Returns:
            dict: {topic_id: [top_words]}
        """
        if self.lda_model is None:
            raise ValueError("Model not fitted yet.")
        
        feature_names = self.vectorizer.get_feature_names_out()
        topics = {}
        
        for topic_idx, topic in enumerate(self.lda_model.components_):
            top_indices = topic.argsort()[-n_words:][::-1]
            top_words = [feature_names[i] for i in top_indices]
            topics[f'Topic {topic_idx}'] = top_words
        
        return topics
    
    def save_model(self, model_path):
        """Save fitted model and vectorizer for later use."""
        model_path = Path(model_path)
        model_path.parent.mkdir(parents=True, exist_ok=True)
        
        with open(model_path, 'wb') as f:
            pickle.dump({
                'vectorizer': self.vectorizer,
                'lda_model': self.lda_model,
                'n_topics': self.n_topics
            }, f)
        
        print(f"✓ Model saved to {model_path}")
    
    def load_model(self, model_path):
        """Load previously fitted model."""
        with open(model_path, 'rb') as f:
            data = pickle.load(f)
        
        self.vectorizer = data['vectorizer']
        self.lda_model = data['lda_model']
        self.n_topics = data['n_topics']
        
        print(f"✓ Model loaded from {model_path}")


# Testing
if __name__ == "__main__":
    print("Testing Specialization Transformer (LDA)")
    print("=" * 80)
    
    # Test data
    test_data = [
        "CURSOS Y/O CAPACITACIÓN EN GESTIÓN PÚBLICA",
        "DIPLOMA Y/O ESPECIALIZACIÓN EN GESTIÓN PUBLICA",
        "CURSO EN GESTIÓN PÚBLICA O DERECHO",
        "NO APLICA",
        "RELACIONADOS CON LA FORMACIÓN ACADÉMICA"
    ]
    
    # Fit and transform
    transformer = SpecializationTransformer(n_topics=3, max_features=50)
    result_df = transformer.fit_transform(test_data)
    
    print("\nTopic distributions:")
    print(result_df.to_string())
    
    print("\nTop words per topic:")
    topics = transformer.get_top_words_per_topic(n_words=5)
    for topic_name, words in topics.items():
        print(f"\n{topic_name}: {', '.join(words)}")
    
    print("\n" + "=" * 80)
    print("✓ Transformer test complete")