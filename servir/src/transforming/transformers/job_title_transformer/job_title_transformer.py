"""
Job Title Transformer - Main orchestrator for matching job titles to ISCO-08.

Pipeline:
1. Read cleaned job titles from servir_cleaned.db
2. Extract unique titles
3. Load ALL ISCO-08 nivel 4 codes
4. Score EVERY ISCO code against each job title using:
   - Semantic similarity
   - Fuzzy matching
5. Save ALL scores to job_title_validation.db for manual validation
"""

import sqlite3
from pathlib import Path
import pandas as pd
from tqdm import tqdm
import sys

# Add parent directory to path for relative imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from job_title_semantic_matcher import JobTitleSemanticMatcher
from job_title_fuzzy_matcher import JobTitleFuzzyMatcher


class JobTitleTransformer:
    """Transform job titles by matching them to ISCO-08 categories."""
    
    def __init__(self, cleaned_db_path, isco_db_path, validation_db_path):
        """
        Initialize transformer with database paths.
        
        Args:
            cleaned_db_path: Path to servir_cleaned.db
            isco_db_path: Path to isco_08_peru.db
            validation_db_path: Path to job_title_validation.db (output)
        """
        self.cleaned_db_path = Path(cleaned_db_path)
        self.isco_db_path = Path(isco_db_path)
        self.validation_db_path = Path(validation_db_path)
        
        # Initialize matchers
        print("Initializing matchers...")
        self.semantic_matcher = JobTitleSemanticMatcher(self.isco_db_path)
        self.fuzzy_matcher = JobTitleFuzzyMatcher(self.isco_db_path)
        
        # Load ALL ISCO codes once
        print("Loading all ISCO-08 nivel 4 codes...")
        self.all_isco_codes = self._load_all_isco_codes()
        print(f"Loaded {len(self.all_isco_codes)} ISCO codes")
    
    def _load_all_isco_codes(self):
        """
        Load all ISCO-08 nivel 4 codes from database.
        
        Returns:
            list of dicts with 'codigo' and 'descripcion'
        """
        conn = sqlite3.connect(self.isco_db_path)
        query = "SELECT codigo, descripcion FROM isco_08_peru WHERE nivel = 4"
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        return df.to_dict('records')
    
    def load_cleaned_titles(self):
        """
        Load all cleaned job titles from servir_cleaned.db.
        
        Returns:
            DataFrame with 'job_title' column
        """
        # Debug: Show which database we're connecting to
        print(f"Connecting to: {self.cleaned_db_path}")
        print(f"File exists: {self.cleaned_db_path.exists()}")
        
        conn = sqlite3.connect(self.cleaned_db_path)
        
        # Debug: Show available tables
        cursor = conn.cursor()
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = cursor.fetchall()
        print(f"Available tables: {tables}")
        
        query = "SELECT DISTINCT job_title FROM cleaned_jobs WHERE job_title IS NOT NULL"
        df = pd.read_sql_query(query, conn)
        conn.close()
        
        print(f"Loaded {len(df)} unique job titles from cleaned database")
        return df
    
    def combine_matches(self, job_title):
        """
        Score ALL ISCO-08 codes against a single job title.
        
        Algorithm:
        1. Get semantic scores for ALL ISCO codes
        2. Get fuzzy scores for ALL ISCO codes
        3. Combine scores for each code
        4. Return ALL codes with both scores
        
        Args:
            job_title (str): Job title to match
        
        Returns:
            list of dicts with ALL ISCO codes and their scores
        """
        # Get ALL matches from both methods
        all_semantic = self.semantic_matcher.match_title(job_title, top_k=999)
        all_fuzzy = self.fuzzy_matcher.match_title(job_title, top_k=999)
        
        # Create lookup dictionaries
        semantic_lookup = {m['codigo']: m['confidence'] for m in all_semantic}
        fuzzy_lookup = {m['codigo']: m['confidence'] for m in all_fuzzy}
        
        # Score ALL ISCO codes
        results = []
        for isco in self.all_isco_codes:
            codigo = isco['codigo']
            descripcion = isco['descripcion']
            
            sem_conf = semantic_lookup.get(codigo, 0)
            fuz_conf = fuzzy_lookup.get(codigo, 0)
            
            results.append({
                'codigo': codigo,
                'descripcion': descripcion,
                'semantic_confidence': sem_conf,
                'fuzzy_confidence': fuz_conf,
                'best_confidence': max(sem_conf, fuz_conf)
            })
        
        # Sort by best confidence (for easier validation, but keeping all)
        ranked = sorted(results, 
                       key=lambda x: x['best_confidence'], 
                       reverse=True)
        
        return ranked
    
    def transform(self):
        """
        Transform all cleaned job titles and save results.
        
        Process:
        1. Load unique cleaned titles
        2. Match each title (semantic + fuzzy combined)
        3. Save to validation database
        """
        # Load cleaned titles
        cleaned_titles_df = self.load_cleaned_titles()
        
        # Process each title
        print(f"\nProcessing {len(cleaned_titles_df)} titles...")
        all_results = []
        
        for _, row in tqdm(cleaned_titles_df.iterrows(), total=len(cleaned_titles_df)):
            job_title = row['job_title']
            
            # Score ALL ISCO codes against this job title
            matches = self.combine_matches(job_title)
            
            for rank, match in enumerate(matches, 1):
                all_results.append({
                    'job_title': job_title,
                    'candidate_codigo': match['codigo'],
                    'candidate_descripcion': match['descripcion'],
                    'semantic_confidence': match['semantic_confidence'],
                    'fuzzy_confidence': match['fuzzy_confidence'],
                    'best_confidence': match['best_confidence'],
                    'rank': rank
                })
        
        # Create results dataframe
        results_df = pd.DataFrame(all_results)
        
        # Save to database
        self.save_validation_db(results_df)
        
        print(f"\n✓ Transformation complete!")
        print(f"  - Processed {len(cleaned_titles_df)} unique titles")
        print(f"  - Created {len(results_df)} candidates")
        print(f"  - Saved to {self.validation_db_path}")
        
        return results_df
    
    def save_validation_db(self, results_df):
        """
        Save matching results to validation database.
        Handles reruns by:
        1. Only processing job titles that aren't already validated
        2. Using INSERT OR REPLACE for unvalidated titles
        
        Args:
            results_df (DataFrame): Results from combine_matches
        """
        self.validation_db_path.parent.mkdir(parents=True, exist_ok=True)
        
        conn = sqlite3.connect(self.validation_db_path)
        
        # Create table if it doesn't exist (don't drop it!)
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
            UNIQUE(job_title, candidate_codigo)
        )
        """)
        
        # Get list of job titles that are ALREADY VALIDATED (don't touch these)
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT job_title FROM fuzzy_matches WHERE validated = 1")
        validated_titles = set(row[0] for row in cursor.fetchall())
        
        if validated_titles:
            print(f"\nSkipping {len(validated_titles)} already-validated job titles...")
        
        # Filter out already-validated titles from results
        new_results = results_df[~results_df['job_title'].isin(validated_titles)]
        
        if len(new_results) == 0:
            print("All job titles already validated. Nothing to update.")
            conn.close()
            return
        
        print(f"Processing {len(new_results['job_title'].unique())} new/unvalidated job titles...")
        
        # Delete old unvalidated matches for these titles (in case of rerun)
        new_titles = tuple(new_results['job_title'].unique())
        if len(new_titles) == 1:
            conn.execute(f"DELETE FROM fuzzy_matches WHERE job_title = ? AND validated = 0", new_titles)
        else:
            placeholders = ','.join('?' * len(new_titles))
            conn.execute(f"DELETE FROM fuzzy_matches WHERE job_title IN ({placeholders}) AND validated = 0", new_titles)
        
        # Insert new results
        for _, row in new_results.iterrows():
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


if __name__ == "__main__":
    # Paths - FIXED: Use proper relative path construction
    from pathlib import Path
    
    # Get the script directory
    script_dir = Path(__file__).resolve().parent
    
    # Navigate to project root
    # Structure: servir/src/transforming/transformers/job_title_transformer/job_title_transformer.py
    # Go up 4 levels: job_title_transformer -> transformers -> transforming -> src -> servir (project root)
    project_root = script_dir.parent.parent.parent.parent
    
    print(f"Script directory: {script_dir}")
    print(f"Project root: {project_root}")
    
    CLEANED_DB = project_root / "servir" / "data" / "cleaned" / "servir_jobs_cleaned.db"
    ISCO_DB = project_root / "servir" / "data" / "reference" / "isco_08_peru.db"
    VALIDATION_DB = script_dir / "data" / "job_title_validation.db"
    
    print(f"\nDatabase paths:")
    print(f"  Cleaned DB: {CLEANED_DB}")
    print(f"  ISCO DB: {ISCO_DB}")
    print(f"  Validation DB: {VALIDATION_DB}")
    print(f"\nFile existence:")
    print(f"  Cleaned DB exists: {CLEANED_DB.exists()}")
    print(f"  ISCO DB exists: {ISCO_DB.exists()}")
    
    # Transform
    transformer = JobTitleTransformer(CLEANED_DB, ISCO_DB, VALIDATION_DB)
    results = transformer.transform()