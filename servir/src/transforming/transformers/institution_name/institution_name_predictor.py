"""
Institution Name Predictor - Predicts MEF IDs for a single institution.

Current implementation: Simple DB lookup (rank=1 best match)
Future implementation: Use fine-tuned sentence transformer model

Usage:
    from institution_name_predictor import predict_mef_ids
    
    mef_ids = predict_mef_ids("Ministerio de Salud", matches_db_path)
"""

import sqlite3
from pathlib import Path


def predict_mef_ids(institution_name, matches_db_path, use_model=False, model_path=None):
    """
    Predict MEF institutional identifiers for a single institution.
    
    Args:
        institution_name (str): Institution name to match
        matches_db_path (Path): Path to institution_name_matches.db
        use_model (bool): If True, use fine-tuned model. If False, use DB lookup.
        model_path (Path): Path to fine-tuned model (if use_model=True)
    
    Returns:
        dict: MEF identifiers {ejecutora, sector, pliego, ...} or None if no match
    """
    
    if use_model:
        # FUTURE: Use fine-tuned model
        if model_path is None or not Path(model_path).exists():
            raise ValueError("Model path required when use_model=True")
        
        # TODO: Implement model-based prediction
        raise NotImplementedError(
            "Model-based prediction not implemented yet. "
            "Run institution_name_finetuner.py first to create a model, "
            "or use use_model=False for simple DB lookup."
        )
    
    else:
        # CURRENT: Simple DB lookup - return best match (rank=1)
        return _predict_from_db(institution_name, matches_db_path)


def _predict_from_db(institution_name, matches_db_path):
    """
    Predict MEF IDs using simple DB lookup (best match = rank 1).
    
    Args:
        institution_name (str): Institution name to match
        matches_db_path (Path): Path to institution_name_matches.db
    
    Returns:
        dict: MEF identifiers or None
    """
    matches_db_path = Path(matches_db_path)
    
    if not matches_db_path.exists():
        raise FileNotFoundError(
            f"Matches database not found: {matches_db_path}\n"
            f"Run institution_name_matcher.py first to create it."
        )
    
    conn = sqlite3.connect(matches_db_path)
    cursor = conn.cursor()
    
    # Get best match (rank=1, highest confidence)
    cursor.execute("""
        SELECT 
            ejecutora,
            ejecutora_nombre,
            nivel_gobierno,
            nivel_gobierno_nombre,
            sector,
            sector_nombre,
            pliego,
            pliego_nombre,
            sec_ejec,
            best_confidence
        FROM institution_matches
        WHERE servir_institution_name = ?
        ORDER BY best_confidence DESC
        LIMIT 1
    """, (institution_name,))
    
    result = cursor.fetchone()
    conn.close()
    
    if result:
        return {
            'ejecutora': result[0],
            'ejecutora_nombre': result[1],
            'nivel_gobierno': result[2],
            'nivel_gobierno_nombre': result[3],
            'sector': result[4],
            'sector_nombre': result[5],
            'pliego': result[6],
            'pliego_nombre': result[7],
            'sec_ejec': result[8],
            'confidence': result[9]
        }
    else:
        # Institution not in matches DB
        return None


def predict_batch(institution_names, matches_db_path, use_model=False, model_path=None):
    """
    Predict MEF IDs for multiple institutions.
    
    Args:
        institution_names (list): List of institution name strings
        matches_db_path (Path): Path to institution_name_matches.db
        use_model (bool): Use fine-tuned model or DB lookup
        model_path (Path): Path to model (if use_model=True)
    
    Returns:
        dict: {institution_name: mef_ids_dict}
    """
    predictions = {}
    
    for inst_name in institution_names:
        mef_ids = predict_mef_ids(inst_name, matches_db_path, use_model, model_path)
        predictions[inst_name] = mef_ids
    
    return predictions


# Example usage
if __name__ == "__main__":
    from pathlib import Path
    
    MATCHES_DB = Path("../../data/transformed/institution_name/institution_name_matches.db")
    
    # Test prediction
    test_institution = "Ministerio de Salud"
    
    print(f"Predicting MEF IDs for: {test_institution}")
    
    try:
        mef_ids = predict_mef_ids(test_institution, MATCHES_DB, use_model=False)
        
        if mef_ids:
            print(f"\nPredicted MEF IDs:")
            print(f"  Ejecutora: {mef_ids['ejecutora']} - {mef_ids['ejecutora_nombre']}")
            print(f"  Sector: {mef_ids['sector']} - {mef_ids['sector_nombre']}")
            print(f"  Confidence: {mef_ids['confidence']}")
        else:
            print("No match found")
            
    except Exception as e:
        print(f"Error: {e}")