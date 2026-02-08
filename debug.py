"""
Debug script to run Job Title Transformer on all cleaned job titles.

This script imports and runs the JobTitleTransformer from the transforming pipeline.
"""

import sys
from pathlib import Path

# Use absolute paths
REPO_ROOT = Path("/Users/francosebastiani/GitHub/Peruvian Civil Service Hiring Patterns Analysis")
sys.path.insert(0, str(REPO_ROOT))

# Import transformer
from servir.src.transforming.transformers.job_title_transformer.job_title_transformer import JobTitleTransformer

# Paths
CLEANED_DB = REPO_ROOT / "servir" / "data" / "cleaned" / "servir_jobs_cleaned.db"
ISCO_DB = REPO_ROOT / "servir" / "data" / "reference" / "isco_08_peru.db"
VALIDATION_DB = REPO_ROOT / "servir" / "src" / "transforming" / "transformers" / "job_title_transformer" / "data" / "job_title_validation.db"

print("=" * 80)
print("Job Title Transformer - Processing All Cleaned Titles")
print("=" * 80)

# Initialize and run transformer
transformer = JobTitleTransformer(CLEANED_DB, ISCO_DB, VALIDATION_DB)
results = transformer.transform()

print("\n" + "=" * 80)
print(f"Results Summary:")
print("=" * 80)
print(f"Total rows in validation DB: {len(results)}")
print(f"\nFirst 10 results:")
print(results.head(10)[['job_title', 'rank', 'candidate_codigo', 'semantic_confidence', 'fuzzy_confidence', 'best_confidence']].to_string(index=False))
print("=" * 80)