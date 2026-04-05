"""
Academic Predictor - Predicts CLASIFICADOR program code for academic profile.

Current implementation: Simple DB lookup (rank=1 best match)
Future implementation: Use fine-tuned sentence transformer model
"""

import sqlite3
from pathlib import Path


def predict_program_code(academic_profile, matches_db_path, use_model=False, model_path=None):
    """
    Predict the best CLASIFICADOR program code for an academic profile.
    
    Args:
        academic_profile (str): Academic profile text
        matches_db_path (Path): Path to academic_matches.db
        use_model (bool): If True, use fine-tuned model
        model_path (Path): Path to fine-tuned model (if use_model=True)
    
    Returns:
        dict: {
            'programa_codigo': str,
            'programa_nombre': str,
            'campo_detallado_codigo': str,
            'thesis_required': 0 or 1,
            'confidence': int
        } or None
    """
    
    if use_model:
        raise NotImplementedError(
            "Model-based prediction not implemented yet. "
            "Run academic_finetuner.py first or use use_model=False."
        )
    
    else:
        return _predict_from_db(academic_profile, matches_db_path)


def _predict_from_db(academic_profile, matches_db_path):
    """
    Predict program code using simple DB lookup (best match = rank 1).
    
    Args:
        academic_profile (str): Academic profile text
        matches_db_path (Path): Path to academic_matches.db
    
    Returns:
        dict or None
    """
    matches_db_path = Path(matches_db_path)
    
    if not matches_db_path.exists():
        raise FileNotFoundError(
            f"Matches database not found: {matches_db_path}\n"
            f"Run academic_matcher.py first."
        )
    
    conn = sqlite3.connect(matches_db_path)
    cursor = conn.cursor()
    
    # Get best match
    cursor.execute("""
        SELECT 
            programa_codigo,
            programa_nombre,
            campo_detallado_codigo,
            campo_detallado_nombre,
            thesis_required,
            best_confidence
        FROM academic_matches
        WHERE servir_academic_profile = ?
        ORDER BY best_confidence DESC
        LIMIT 1
    """, (academic_profile,))
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return {
            'programa_codigo': result[0],
            'programa_nombre': result[1],
            'campo_detallado_codigo': result[2],
            'campo_detallado_nombre': result[3],
            'thesis_required': result[4],
            'confidence': result[5]
        }
    else:
        return None


def predict_batch(academic_profiles, matches_db_path, use_model=False, model_path=None):
    """
    Predict program codes for multiple academic profiles.
    
    Args:
        academic_profiles (list): List of academic profile strings
        matches_db_path (Path): Path to academic_matches.db
        use_model (bool): Use model or DB lookup
        model_path (Path): Path to model
    
    Returns:
        dict: {academic_profile: prediction_dict}
    """
    predictions = {}
    
    for profile in academic_profiles:
        pred = predict_program_code(profile, matches_db_path, use_model, model_path)
        predictions[profile] = pred
    
    return predictions