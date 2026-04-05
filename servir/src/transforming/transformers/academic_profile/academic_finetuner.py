"""
Academic Finetuner - Fine-tunes sentence transformer on validated data.

Trains a domain-specific model using manual corrections from academic_matches.db.

Process:
1. Load validated academic_profile → program pairs
2. Fine-tune Spanish sentence transformer
3. Save improved model weights
4. Evaluate performance

Usage:
    python academic_finetuner.py
"""

import sqlite3
import pandas as pd
from pathlib import Path
import sys


def load_validated_data(matches_db_path):
    """
    Load validated academic profile → program mappings.
    
    Args:
        matches_db_path (Path): Path to academic_matches.db
    
    Returns:
        DataFrame with validated mappings
    """
    if not Path(matches_db_path).exists():
        raise FileNotFoundError(f"Matches database not found: {matches_db_path}")
    
    conn = sqlite3.connect(matches_db_path)
    
    query = """
    SELECT DISTINCT
        servir_academic_profile,
        validated_programa_codigo,
        validated_programa_nombre
    FROM academic_matches
    WHERE validated = 1
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"Loaded {len(df)} validated academic profile mappings")
    
    return df


def finetune_model(matches_db_path, output_model_path, base_model='hiiamsid/sentence_similarity_spanish_es'):
    """
    Fine-tune sentence transformer on validated academic data.
    
    Args:
        matches_db_path (Path): Path to academic_matches.db with validated data
        output_model_path (Path): Where to save fine-tuned model
        base_model (str): Base model to fine-tune
    
    Returns:
        Path to saved model
    """
    
    # Load validated data
    print("Loading validated data...")
    validated_df = load_validated_data(matches_db_path)
    
    if len(validated_df) < 100:
        print(f"⚠ Warning: Only {len(validated_df)} validated samples found.")
        print("Recommended: At least 500 validated samples for effective fine-tuning.")
        proceed = input("Continue anyway? (yes/no): ")
        if proceed.lower() != 'yes':
            print("Fine-tuning cancelled.")
            return None
    
    # TODO: Implement fine-tuning
    raise NotImplementedError(
        "Fine-tuning not implemented yet.\n\n"
        "Implementation plan:\n"
        "1. Load base sentence transformer model\n"
        "2. Create training pairs (servir_profile, clasificador_program_name)\n"
        "3. Use contrastive learning loss\n"
        "4. Train for N epochs\n"
        "5. Evaluate on validation set\n"
        "6. Save model to output_model_path\n\n"
        "For now, use academic_predictor with use_model=False (DB lookup)."
    )


if __name__ == "__main__":
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent.parent.parent
    
    MATCHES_DB = project_root / "servir" / "data" / "transformed" / "academic" / "academic_matches.db"
    MODEL_OUTPUT = script_dir / "models" / "finetuned_academic_model"
    
    print("=" * 80)
    print("Academic Model Fine-tuning")
    print("=" * 80)
    
    try:
        finetune_model(MATCHES_DB, MODEL_OUTPUT)
    except NotImplementedError as e:
        print(f"\n{e}")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)