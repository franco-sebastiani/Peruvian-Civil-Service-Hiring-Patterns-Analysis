"""
Job Title Matcher - Main orchestrator for matching job titles to ISCO-08.

Pipeline:
1. Read cleaned job titles from servir_jobs_cleaned.db
2. Extract unique titles (optionally sample)
3. Load ALL ISCO-08 nivel 4 codes
4. Score EVERY ISCO code against each job title using:
   - Semantic similarity
   - Fuzzy matching
5. Save ALL scores to job_title_matches.db for manual validation

CLEAN ARCHITECTURE:
- This file: Pure business logic (matching algorithms)
- queries.py: SQL queries
- operations.py: Database operations
- schema.py: Table definitions
"""

import sys
from pathlib import Path
import pandas as pd
from tqdm import tqdm

# Add parent directory to path for relative imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from job_title_semantic_matcher import JobTitleSemanticMatcher
from job_title_fuzzy_matcher import JobTitleFuzzyMatcher

# Import database operations
# Import database operations - use absolute path
sys.path.insert(0, "/Users/francosebastiani/Documents/GitHub/Peruvian-Civil-Service-Hiring-Patterns-Analysis")
from servir.data.transformed.job_title import operations, queries


class JobTitleMatcher:
    """Match job titles to ISCO-08 categories using dual semantic + fuzzy approach."""
    
    def __init__(self, cleaned_db_path, isco_db_path, matches_db_path):
        """
        Initialize matcher with database paths.
        
        Args:
            cleaned_db_path: Path to servir_jobs_cleaned.db
            isco_db_path: Path to isco_08_peru.db
            matches_db_path: Path to job_title_matches.db (output)
        """
        self.cleaned_db_path = Path(cleaned_db_path)
        self.isco_db_path = Path(isco_db_path)
        self.matches_db_path = Path(matches_db_path)
        
        # Initialize matchers
        print("Initializing matchers...")
        self.semantic_matcher = JobTitleSemanticMatcher(self.isco_db_path)
        self.fuzzy_matcher = JobTitleFuzzyMatcher(self.isco_db_path)
        
        # Load ALL ISCO codes once
        print("Loading all ISCO-08 nivel 4 codes...")
        self.all_isco_codes = queries.load_isco_codes(self.isco_db_path)
        print(f"Loaded {len(self.all_isco_codes)} ISCO codes")
    
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
    
    def match(self, sample_size=None):
        """
        Match job titles to ISCO codes and save results.
        
        Process:
        1. Load unique cleaned titles
        2. Optionally sample a subset for testing
        3. Check which already exist in matches DB
        4. Match only NEW titles that don't exist yet
        5. Save to matches database
        
        Args:
            sample_size (int): If provided, randomly sample this many titles.
                              If None, process all titles.
        """
        # Get processing status
        status = operations.get_processing_status(
            self.cleaned_db_path,
            self.matches_db_path
        )
        
        all_titles = status['all_titles']
        existing_titles = status['existing_titles']
        titles_to_process = status['titles_to_process']
        
        print(f"\nLoaded {len(all_titles)} unique job titles from cleaned database")
        
        if len(existing_titles) > 0:
            print(f"Found {len(existing_titles)} job titles already in matches DB")
        
        if len(titles_to_process) == 0:
            print("\n✓ All job titles already in matches DB. Nothing to process!")
            return pd.DataFrame()
        
        # Apply sampling if requested
        if sample_size and sample_size < len(titles_to_process):
            titles_to_process = titles_to_process.sample(n=sample_size, random_state=42)
            print(f"Sampling {sample_size} random titles for processing...")
        
        print(f"\nProcessing {len(titles_to_process)} NEW titles...")
        all_results = []
        
        for _, row in tqdm(titles_to_process.iterrows(), total=len(titles_to_process)):
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
        
        # Save to database using operations module
        operations.save_matches(self.matches_db_path, results_df)
        
        print(f"\n✓ Matching complete!")
        print(f"  - Processed {len(titles_to_process)} unique titles")
        print(f"  - Created {len(results_df)} match candidates")
        print(f"  - Saved to {self.matches_db_path}")
        
        return results_df


if __name__ == "__main__":
    # Paths
    from pathlib import Path
    
    # Get the script directory
    script_dir = Path(__file__).resolve().parent
    
    # Navigate to project root
    project_root = script_dir.parent.parent.parent.parent.parent
    
    print(f"Script directory: {script_dir}")
    print(f"Project root: {project_root}")
    
    CLEANED_DB = project_root / "servir" / "data" / "cleaned" / "servir_jobs_cleaned.db"
    ISCO_DB = project_root / "servir" / "data" / "reference" / "ISCO_08" / "isco_08_peru.db"
    MATCHES_DB = project_root / "servir" / "data" / "transformed" / "job_title" / "job_title_matches.db"
    
    print(f"\nDatabase paths:")
    print(f"  Cleaned DB: {CLEANED_DB}")
    print(f"  ISCO DB: {ISCO_DB}")
    print(f"  Matches DB: {MATCHES_DB}")
    print(f"\nFile existence:")
    print(f"  Cleaned DB exists: {CLEANED_DB.exists()}")
    print(f"  ISCO DB exists: {ISCO_DB.exists()}")
    
    # Match
    matcher = JobTitleMatcher(CLEANED_DB, ISCO_DB, MATCHES_DB)
    results = matcher.match()  # Or: matcher.match(sample_size=100)