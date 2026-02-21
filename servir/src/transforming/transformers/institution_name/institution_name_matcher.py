"""
Institution Name Matcher - Orchestrator for matching SERVIR institutions to MEF.

Pipeline:
1. Load SERVIR institutions from cleaned database
2. Load MEF institution catalog from local database
3. Score each SERVIR institution against ALL MEF institutions using:
   - Semantic similarity
   - Fuzzy matching
4. Store ALL MEF identifiers (nivel_gobierno, sector, pliego, ejecutora, etc.)
5. Save to institution_name_matches.db for manual validation

Output: Comprehensive lookup table linking SERVIR ↔ MEF with all institutional IDs.
"""

import sys
from pathlib import Path
import pandas as pd
from tqdm import tqdm

# Add parent directory to path for relative imports
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from institution_name_semantic_matcher import InstitutionSemanticMatcher
from institution_name_fuzzy_matcher import InstitutionFuzzyMatcher

# Import database operations
project_root = current_dir.parent.parent.parent.parent
sys.path.insert(0, str(project_root))
from servir.data.transformed.institution_name import operations


class InstitutionNameMatcher:
    """Match SERVIR institutions to MEF institution catalog using dual approach."""
    
    def __init__(self, cleaned_db_path, mef_db_path, matches_db_path):
        """
        Initialize matcher with database paths.
        
        Args:
            cleaned_db_path: Path to servir_jobs_cleaned.db
            mef_db_path: Path to mef_budget.db (local MEF database)
            matches_db_path: Path to institution_name_matches.db (output)
        """
        self.cleaned_db_path = Path(cleaned_db_path)
        self.mef_db_path = Path(mef_db_path)
        self.matches_db_path = Path(matches_db_path)
        
        # Load MEF institution catalog from local database
        print(f"\nLoading MEF institution catalog from: {mef_db_path}...")
        self.mef_institutions = operations.fetch_and_cache_mef_catalog(self.mef_db_path)
        
        # Initialize matchers
        print("\nInitializing matchers...")
        self.semantic_matcher = InstitutionSemanticMatcher(self.mef_institutions)
        self.fuzzy_matcher = InstitutionFuzzyMatcher(self.mef_institutions)
    
    def combine_matches(self, servir_institution_name):
        """
        Score a SERVIR institution against ALL MEF institutions using both methods.
        
        Algorithm:
        1. Get semantic scores for ALL MEF institutions
        2. Get fuzzy scores for ALL MEF institutions
        3. Combine scores for each unique MEF ejecutora (deduplicate)
        4. Return ALL unique institutions ranked by best score
        
        Args:
            servir_institution_name (str): Institution name from SERVIR
        
        Returns:
            list of dicts with ALL unique MEF institutions and their scores
        """
        # Get matches from both methods - request ALL (top_k=999 should cover all)
        semantic_matches = self.semantic_matcher.match_institution(
            servir_institution_name, 
            top_k=999
        )
        fuzzy_matches = self.fuzzy_matcher.match_institution(
            servir_institution_name, 
            top_k=999
        )
        
        # Create lookup dictionaries by ejecutora code
        semantic_lookup = {m['ejecutora']: m['confidence'] for m in semantic_matches}
        fuzzy_lookup = {m['ejecutora']: m['confidence'] for m in fuzzy_matches}
        
        # Score ALL UNIQUE MEF institutions (deduplicate by ejecutora)
        # Group by ejecutora and take first occurrence
        unique_ejecutoras = self.mef_institutions.drop_duplicates(subset=['EJECUTORA'], keep='first')
        
        combined = []
        for idx, mef_record in unique_ejecutoras.iterrows():
            ejecutora = mef_record.get('EJECUTORA')
            
            sem_conf = semantic_lookup.get(ejecutora, 0)
            fuz_conf = fuzzy_lookup.get(ejecutora, 0)
            
            combined.append({
                'servir_institution_name': servir_institution_name,
                'ejecutora': ejecutora,
                'ejecutora_nombre': mef_record.get('EJECUTORA_NOMBRE'),
                'nivel_gobierno': mef_record.get('NIVEL_GOBIERNO'),
                'nivel_gobierno_nombre': mef_record.get('NIVEL_GOBIERNO_NOMBRE'),
                'sector': mef_record.get('SECTOR'),
                'sector_nombre': mef_record.get('SECTOR_NOMBRE'),
                'pliego': mef_record.get('PLIEGO'),
                'pliego_nombre': mef_record.get('PLIEGO_NOMBRE'),
                'sec_ejec': mef_record.get('SEC_EJEC'),
                'departamento_ejecutora': mef_record.get('DEPARTAMENTO_EJECUTORA'),
                'departamento_ejecutora_nombre': mef_record.get('DEPARTAMENTO_EJECUTORA_NOMBRE'),
                'provincia_ejecutora': mef_record.get('PROVINCIA_EJECUTORA'),
                'provincia_ejecutora_nombre': mef_record.get('PROVINCIA_EJECUTORA_NOMBRE'),
                'distrito_ejecutora': mef_record.get('DISTRITO_EJECUTORA'),
                'distrito_ejecutora_nombre': mef_record.get('DISTRITO_EJECUTORA_NOMBRE'),
                'semantic_confidence': sem_conf,
                'fuzzy_confidence': fuz_conf,
                'best_confidence': max(sem_conf, fuz_conf)
            })
        
        # Sort by best confidence
        ranked = sorted(combined, key=lambda x: x['best_confidence'], reverse=True)
        
        # Add rank
        for i, match in enumerate(ranked, 1):
            match['rank'] = i
        
        return ranked
    
    def match(self, sample_size=None):
        """
        Match SERVIR institutions to MEF and save results.
        
        Process:
        1. Load SERVIR institutions
        2. Optionally sample a subset for testing
        3. Check which already exist in matches DB
        4. Match only NEW institutions
        5. Save comprehensive results with all MEF identifiers
        
        Args:
            sample_size (int): If provided, randomly sample this many institutions.
                              If None, process all institutions.
        """
        # Get processing status
        status = operations.get_processing_status(
            self.cleaned_db_path,
            self.matches_db_path
        )
        
        all_institutions = status['all_institutions']
        existing_institutions = status['existing_institutions']
        institutions_to_process = status['institutions_to_process']
        
        print(f"\nLoaded {len(all_institutions)} unique institutions from SERVIR")
        
        if len(existing_institutions) > 0:
            print(f"Found {len(existing_institutions)} institutions already in matches DB")
        
        if len(institutions_to_process) == 0:
            print("\n✓ All institutions already in matches DB. Nothing to process!")
            return pd.DataFrame()
        
        # Apply sampling if requested
        if sample_size and sample_size < len(institutions_to_process):
            institutions_to_process = institutions_to_process.sample(n=sample_size, random_state=42)
            print(f"Sampling {sample_size} random institutions for processing...")
        
        print(f"\nProcessing {len(institutions_to_process)} NEW institutions...")
        all_results = []
        
        for _, row in tqdm(institutions_to_process.iterrows(), total=len(institutions_to_process)):
            institution_name = row['institution']
            
            # Match against ALL MEF institutions
            matches = self.combine_matches(institution_name)
            
            # Add all matches to results
            all_results.extend(matches)
        
        # Create results dataframe
        results_df = pd.DataFrame(all_results)
        
        # Save to database using operations module
        operations.save_matches(self.matches_db_path, results_df)
        
        print(f"\n✓ Matching complete!")
        print(f"  - Processed {len(institutions_to_process)} unique institutions")
        print(f"  - Created {len(results_df)} match candidates")
        print(f"  - Saved to {self.matches_db_path}")
        
        return results_df


if __name__ == "__main__":
    # Paths
    from pathlib import Path
    
    # Get the script directory
    script_dir = Path(__file__).resolve().parent
    
    # Navigate to project root
    project_root = script_dir.parent.parent.parent.parent
    
    print(f"Script directory: {script_dir}")
    print(f"Project root: {project_root}")
    
    CLEANED_DB = project_root / "servir" / "data" / "cleaned" / "servir_jobs_cleaned.db"
    MEF_DB = project_root / "servir" / "data" / "reference" / "MEF_budget" / "mef_budget.db"
    MATCHES_DB = project_root / "servir" / "data" / "transformed" / "institution_name" / "institution_name_matches.db"
    
    print(f"\nDatabase paths:")
    print(f"  Cleaned DB: {CLEANED_DB}")
    print(f"  MEF DB: {MEF_DB}")
    print(f"  Matches DB: {MATCHES_DB}")
    print(f"\nFile existence:")
    print(f"  Cleaned DB exists: {CLEANED_DB.exists()}")
    print(f"  MEF DB exists: {MEF_DB.exists()}")
    
    if not MEF_DB.exists():
        print("\n✗ MEF database not found!")
        print("Run this first: python servir/data/reference/MEF_budget/load_mef_to_sqlite.py")
        sys.exit(1)
    
    # Match
    matcher = InstitutionNameMatcher(CLEANED_DB, MEF_DB, MATCHES_DB)
    results = matcher.match()  # Or: matcher.match(sample_size=50)