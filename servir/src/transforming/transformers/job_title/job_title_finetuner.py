"""
Job Title Finetuner - Fine-tunes sentence transformer on validated data.

Trains a domain-specific model using manual corrections from job_title_matches.db.

Process:
1. Load validated job_title → ISCO code pairs
2. Fine-tune Spanish sentence transformer
3. Save improved model weights
4. Evaluate performance on holdout set

Usage:
    python job_title_finetuner.py
"""

import sqlite3
import pandas as pd
from pathlib import Path
import sys

# TODO: Add these imports when implementing
# from sentence_transformers import SentenceTransformer, InputExample, losses
# from torch.utils.data import DataLoader


def load_validated_data(matches_db_path):
    """
    Load validated job title → ISCO mappings from database.
    
    Args:
        matches_db_path (Path): Path to job_title_matches.db
    
    Returns:
        DataFrame with 'job_title' and 'validated_codigo' columns
    """
    if not Path(matches_db_path).exists():
        raise FileNotFoundError(f"Matches database not found: {matches_db_path}")
    
    conn = sqlite3.connect(matches_db_path)
    
    query = """
    SELECT DISTINCT
        job_title,
        validated_codigo,
        validated_descripcion
    FROM job_title_matches
    WHERE validated = 1
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"Loaded {len(df)} validated job title mappings")
    
    return df


def finetune_model(matches_db_path, output_model_path, base_model='hiiamsid/sentence_similarity_spanish_es'):
    """
    Fine-tune sentence transformer on validated job title data.
    
    Args:
        matches_db_path (Path): Path to job_title_matches.db with validated data
        output_model_path (Path): Where to save fine-tuned model
        base_model (str): Base model to fine-tune
    
    Returns:
        Path to saved model
    """
    
    # Load validated data
    print("Loading validated data...")
    validated_df = load_validated_data(matches_db_path)
    
    if len(validated_df) < 50:
        print(f"⚠ Warning: Only {len(validated_df)} validated samples found.")
        print("Recommended: At least 500 validated samples for effective fine-tuning.")
        proceed = input("Continue anyway? (yes/no): ")
        if proceed.lower() != 'yes':
            print("Fine-tuning cancelled.")
            return None
    
    # TODO: Implement fine-tuning
    # This is a placeholder for the actual implementation
    raise NotImplementedError(
        "Fine-tuning not implemented yet.\n\n"
        "Implementation plan:\n"
        "1. Load base sentence transformer model\n"
        "2. Create training pairs (job_title, ISCO_description)\n"
        "3. Use contrastive learning loss\n"
        "4. Train for N epochs\n"
        "5. Evaluate on validation set\n"
        "6. Save model to output_model_path\n\n"
        "For now, use job_title_predictor with use_model=False (DB lookup)."
    )
    
    # FUTURE IMPLEMENTATION OUTLINE:
    """
    # Load base model
    model = SentenceTransformer(base_model)
    
    # Create training examples
    # Each example: (job_title, correct_ISCO_description) are similar
    train_examples = []
    for _, row in validated_df.iterrows():
        train_examples.append(InputExample(
            texts=[row['job_title'], row['validated_descripcion']]
        ))
    
    # Create dataloader
    train_dataloader = DataLoader(train_examples, shuffle=True, batch_size=16)
    
    # Define loss function (contrastive learning)
    train_loss = losses.MultipleNegativesRankingLoss(model)
    
    # Train
    model.fit(
        train_objectives=[(train_dataloader, train_loss)],
        epochs=10,
        warmup_steps=100
    )
    
    # Save model
    output_model_path.parent.mkdir(parents=True, exist_ok=True)
    model.save(str(output_model_path))
    
    print(f"✓ Model saved to: {output_model_path}")
    
    return output_model_path
    """


if __name__ == "__main__":
    # Paths
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent.parent.parent
    
    MATCHES_DB = project_root / "servir" / "data" / "transformed" / "job_title" / "job_title_matches.db"
    MODEL_OUTPUT = script_dir / "models" / "finetuned_job_title_model"
    
    print("=" * 80)
    print("Job Title Model Fine-tuning")
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