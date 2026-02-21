"""
Job Title Predictor - Predicts ISCO code for a single job title.

Current implementation: Simple DB lookup (rank=1 best match)
Future implementation: Use fine-tuned sentence transformer model

Usage:
    from job_title_predictor import predict_isco_code
    
    isco_code = predict_isco_code("Abogado", matches_db_path)
"""

import sqlite3
from pathlib import Path


def predict_isco_code(job_title, matches_db_path, use_model=False, model_path=None):
    """
    Predict the best ISCO code for a single job title.
    
    Args:
        job_title (str): Job title to classify
        matches_db_path (Path): Path to job_title_matches.db
        use_model (bool): If True, use fine-tuned model. If False, use DB lookup.
        model_path (Path): Path to fine-tuned model (if use_model=True)
    
    Returns:
        str: ISCO code (e.g., "2619") or None if no match found
    """
    
    if use_model:
        # FUTURE: Use fine-tuned model
        if model_path is None or not Path(model_path).exists():
            raise ValueError("Model path required when use_model=True")
        
        # TODO: Implement model-based prediction
        raise NotImplementedError(
            "Model-based prediction not implemented yet. "
            "Run job_title_finetuner.py first to create a model, "
            "or use use_model=False for simple DB lookup."
        )
    
    else:
        # CURRENT: Simple DB lookup - return best match (rank=1)
        return _predict_from_db(job_title, matches_db_path)


def _predict_from_db(job_title, matches_db_path):
    """
    Predict ISCO code using simple DB lookup (best match = rank 1).
    
    Args:
        job_title (str): Job title to match
        matches_db_path (Path): Path to job_title_matches.db
    
    Returns:
        str: ISCO code or None
    """
    matches_db_path = Path(matches_db_path)
    
    if not matches_db_path.exists():
        raise FileNotFoundError(
            f"Matches database not found: {matches_db_path}\n"
            f"Run job_title_matcher.py first to create it."
        )
    
    conn = sqlite3.connect(matches_db_path)
    cursor = conn.cursor()
    
    # Get best match (rank=1, highest confidence)
    cursor.execute("""
        SELECT candidate_codigo, candidate_descripcion, best_confidence
        FROM job_title_matches
        WHERE job_title = ?
        ORDER BY best_confidence DESC
        LIMIT 1
    """, (job_title,))
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        isco_code, isco_desc, confidence = result
        return isco_code
    else:
        # Job title not in matches DB
        return None


def predict_batch(job_titles, matches_db_path, use_model=False, model_path=None):
    """
    Predict ISCO codes for multiple job titles.
    
    Args:
        job_titles (list): List of job title strings
        matches_db_path (Path): Path to job_title_matches.db
        use_model (bool): Use fine-tuned model or DB lookup
        model_path (Path): Path to model (if use_model=True)
    
    Returns:
        dict: {job_title: isco_code}
    """
    predictions = {}
    
    for job_title in job_titles:
        isco_code = predict_isco_code(job_title, matches_db_path, use_model, model_path)
        predictions[job_title] = isco_code
    
    return predictions


# Example usage
if __name__ == "__main__":
    from pathlib import Path
    
    MATCHES_DB = Path("../../data/transformed/job_title/job_title_matches.db")
    
    # Test prediction
    test_title = "Abogado"
    
    print(f"Predicting ISCO code for: {test_title}")
    
    try:
        isco_code = predict_isco_code(test_title, MATCHES_DB, use_model=False)
        print(f"Predicted ISCO code: {isco_code}")
    except Exception as e:
        print(f"Error: {e}")