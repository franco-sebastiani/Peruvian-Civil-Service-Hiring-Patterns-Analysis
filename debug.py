"""
Test script combining semantic and fuzzy matching independently.

Algorithm:
1. Run semantic matcher (top-5)
2. Score those 5 candidates with fuzzy
3. Run fuzzy matcher (top-5)
4. Score fuzzy-only candidates (not in semantic top-5) with semantic
5. Combine all candidates, re-rank by best score (max of both)
6. Return top-3 overall
7. Save to database with both scores
"""

import sys
from pathlib import Path
import pandas as pd
import sqlite3

# Use absolute paths
REPO_ROOT = Path("/Users/francosebastiani/GitHub/Peruvian Civil Service Hiring Patterns Analysis")
sys.path.insert(0, str(REPO_ROOT))

# Import both independent matchers
from servir.src.transforming.transformers.job_title_transformer.job_title_semantic_matcher import JobTitleSemanticMatcher
from servir.src.transforming.transformers.job_title_transformer.job_title_fuzzy_matcher import JobTitleFuzzyMatcher

# Paths
ISCO_DB = REPO_ROOT / "servir" / "data" / "reference" / "isco_08_peru.db"
VALIDATION_DB = REPO_ROOT / "servir" / "src" / "transforming" / "transformers" / "job_title_transformer" / "data" / "job_title_validation.db"

print("=" * 80)
print("Combining Semantic + Fuzzy Matchers (Independent Approaches)")
print("=" * 80)

# Initialize both matchers
print(f"\n1. Initializing matchers...")
semantic_matcher = JobTitleSemanticMatcher(ISCO_DB)
fuzzy_matcher = JobTitleFuzzyMatcher(ISCO_DB)

# Test sample titles
print("\n2. Processing sample titles...\n")
sample_titles = pd.DataFrame({
    'job_title': [
        "Abogado Senior",
        "Ingeniero de Sistemas",
        "Contador",
        "Profesor de Primaria",
        "Médico General"
    ]
})

# Process each title
all_results = []

for _, row in sample_titles.iterrows():
    job_title = row['job_title']
    print(f"Processing: '{job_title}'")
    
    # Step 1: Get semantic top-5
    semantic_matches = semantic_matcher.match_title(job_title, top_k=5)
    semantic_codes = {m['codigo']: m['confidence'] for m in semantic_matches}
    
    # Step 2: Score semantic candidates with fuzzy
    candidates = {}
    for sem_match in semantic_matches:
        codigo = sem_match['codigo']
        sem_conf = sem_match['confidence']
        
        # Find fuzzy score for this codigo
        fuzzy_matches_all = fuzzy_matcher.match_title(job_title, top_k=999)
        fuz_match = next((m for m in fuzzy_matches_all if m['codigo'] == codigo), None)
        fuz_conf = fuz_match['confidence'] if fuz_match else 0
        
        candidates[codigo] = {
            'codigo': codigo,
            'descripcion': sem_match['descripcion'],
            'semantic_confidence': sem_conf,
            'fuzzy_confidence': fuz_conf,
            'best_confidence': max(sem_conf, fuz_conf)
        }
    
    # Step 3: Get fuzzy top-5
    fuzzy_matches = fuzzy_matcher.match_title(job_title, top_k=5)
    
    # Step 4: Score fuzzy-only candidates with semantic
    for fuz_match in fuzzy_matches:
        codigo = fuz_match['codigo']
        if codigo not in candidates:  # Only process new codes
            fuz_conf = fuz_match['confidence']
            
            # Find semantic score for this codigo
            semantic_matches_all = semantic_matcher.match_title(job_title, top_k=999)
            sem_match = next((m for m in semantic_matches_all if m['codigo'] == codigo), None)
            sem_conf = sem_match['confidence'] if sem_match else 0
            
            candidates[codigo] = {
                'codigo': codigo,
                'descripcion': fuz_match['descripcion'],
                'semantic_confidence': sem_conf,
                'fuzzy_confidence': fuz_conf,
                'best_confidence': max(sem_conf, fuz_conf)
            }
    
    # Step 5 & 6: Re-rank by best_confidence and get top-3
    ranked = sorted(candidates.values(), 
                   key=lambda x: x['best_confidence'], 
                   reverse=True)[:3]
    
    for rank, match in enumerate(ranked, 1):
        all_results.append({
            'job_title': job_title,
            'candidate_codigo': match['codigo'],
            'candidate_descripcion': match['descripcion'],
            'semantic_confidence': match['semantic_confidence'],
            'fuzzy_confidence': match['fuzzy_confidence'],
            'best_confidence': match['best_confidence'],
            'rank': rank
        })
        print(f"  {rank}. {match['codigo']} - {match['descripcion'][:50]}")
        print(f"     Semantic: {match['semantic_confidence']}, Fuzzy: {match['fuzzy_confidence']}, Best: {match['best_confidence']}")

# Create results dataframe
results_df = pd.DataFrame(all_results)

# Display results table
print("\n3. Combined Results Table:")
print(results_df[['job_title', 'rank', 'candidate_codigo', 'semantic_confidence', 'fuzzy_confidence', 'best_confidence']].to_string(index=False))

# Step 7: Save to database
print(f"\n4. Creating validation database: {VALIDATION_DB}")
VALIDATION_DB.parent.mkdir(parents=True, exist_ok=True)

conn = sqlite3.connect(VALIDATION_DB)

# Drop old table if exists
conn.execute("DROP TABLE IF EXISTS fuzzy_matches")

# Create table with both scores
conn.execute("""
CREATE TABLE IF NOT EXISTS fuzzy_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_title TEXT NOT NULL,
    candidate_codigo TEXT NOT NULL,
    candidate_descripcion TEXT NOT NULL,
    semantic_confidence INTEGER,
    fuzzy_confidence INTEGER,
    best_confidence INTEGER,
    rank INTEGER NOT NULL,
    validated INTEGER DEFAULT 0,
    validated_codigo TEXT,
    validated_descripcion TEXT,
    notes TEXT,
    UNIQUE(job_title, candidate_codigo, rank)
)
""")

# Insert results
for _, row in results_df.iterrows():
    conn.execute("""
    INSERT INTO fuzzy_matches 
    (job_title, candidate_codigo, candidate_descripcion, semantic_confidence, fuzzy_confidence, best_confidence, rank)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        row['job_title'],
        row['candidate_codigo'],
        row['candidate_descripcion'],
        int(row['semantic_confidence']),
        int(row['fuzzy_confidence']),
        int(row['best_confidence']),
        row['rank']
    ))

conn.commit()
conn.close()
print(f"✓ Validation DB created: {VALIDATION_DB}")

print("\n" + "=" * 80)
print("✓ Test completed successfully!")
print("=" * 80)