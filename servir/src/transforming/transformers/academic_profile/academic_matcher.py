"""
Academic Matcher - Orchestrator for matching SERVIR academic profiles to CLASIFICADOR.

Pipeline:
1. Parse SERVIR academic profiles (extract level + flags)
2. Load CLASIFICADOR program catalog
3. Filter programs by detected academic level
4. Score each profile against relevant programs using:
   - Semantic similarity
   - Fuzzy matching
5. Store ALL matches with parsed flags to academic_matches.db

Output: Comprehensive matching database for manual validation.
"""

import sys
from pathlib import Path
import pandas as pd
from tqdm import tqdm

# Add parent directory to path
current_dir = Path(__file__).parent
sys.path.insert(0, str(current_dir))

from academic_parser import parse_academic_profile
from academic_semantic_matcher import AcademicSemanticMatcher
from academic_fuzzy_matcher import AcademicFuzzyMatcher

# Import database operations
project_root = current_dir.parent.parent.parent.parent.parent
sys.path.insert(0, str(project_root))
from servir.data.transformed.academic import operations


class AcademicMatcher:
    """Match SERVIR academic profiles to CLASIFICADOR programs."""
    
    def __init__(self, cleaned_db_path, clasificador_db_path, matches_db_path):
        """
        Initialize matcher with database paths.
        
        Args:
            cleaned_db_path: Path to servir_jobs_cleaned.db
            clasificador_db_path: Path to clasificador_carreras.db
            matches_db_path: Path to academic_matches.db (output)
        """
        self.cleaned_db_path = Path(cleaned_db_path)
        self.clasificador_db_path = Path(clasificador_db_path)
        self.matches_db_path = Path(matches_db_path)
        
        # Load CLASIFICADOR catalog
        print(f"\nLoading CLASIFICADOR catalog from: {clasificador_db_path}...")
        self.clasificador_programs = operations.load_clasificador_catalog(self.clasificador_db_path)
        
        # Initialize matchers
        print("\nInitializing matchers...")
        self.semantic_matcher = AcademicSemanticMatcher(self.clasificador_programs)
        self.fuzzy_matcher = AcademicFuzzyMatcher(self.clasificador_programs)
    
    def combine_matches(self, servir_academic_profile, parsed_info):
        """
        Match academic profile to CLASIFICADOR programs using both methods.
        
        Args:
            servir_academic_profile (str): Academic profile from SERVIR
            parsed_info (dict): Parsed level and flags from academic_parser
        
        Returns:
            list of dicts with combined match results
        """
        search_levels = parsed_info['search_levels']
        
        # If no search levels (e.g., secundaria), return empty
        if not search_levels or len(search_levels) == 0:
            return []
        
        # Get matches from both methods
        semantic_matches = self.semantic_matcher.match_profile(
            servir_academic_profile,
            search_levels=search_levels,
            top_k=999  # Get all for comprehensive matching
        )
        
        fuzzy_matches = self.fuzzy_matcher.match_profile(
            servir_academic_profile,
            search_levels=search_levels,
            top_k=999
        )
        
        # Create lookup dictionaries
        semantic_lookup = {m['programa_codigo']: m['confidence'] for m in semantic_matches}
        fuzzy_lookup = {m['programa_codigo']: m['confidence'] for m in fuzzy_matches}
        
        # Get all unique program codes from both methods
        all_program_codes = set()
        all_program_codes.update(semantic_lookup.keys())
        all_program_codes.update(fuzzy_lookup.keys())
        
        # Build combined results
        combined = []
        for program_code in all_program_codes:
            # Get full program info
            program_row = self.clasificador_programs[
                self.clasificador_programs['programa_codigo'] == program_code
            ]
            
            if len(program_row) == 0:
                continue
            
            program_row = program_row.iloc[0]
            
            sem_conf = semantic_lookup.get(program_code, 0)
            fuz_conf = fuzzy_lookup.get(program_code, 0)
            
            combined.append({
                'servir_academic_profile': servir_academic_profile,
                'academic_level_code': parsed_info.get('level_name'),  # Store extracted level
                'thesis_required': parsed_info.get('thesis_required', 0),
                'accepts_related_fields': parsed_info.get('accepts_related_fields', 0),
                'requires_colegiado': parsed_info.get('requires_colegiado', 0),
                'requires_habilitado': parsed_info.get('requires_habilitado', 0),
                'multiple_options_allowed': parsed_info.get('multiple_options_allowed', 0),
                'programa_codigo': program_row.get('programa_codigo'),
                'programa_nombre': program_row.get('programa_nombre'),
                'campo_amplio_codigo': program_row.get('campo_amplio_codigo'),
                'campo_amplio_nombre': program_row.get('campo_amplio_nombre'),
                'campo_especifico_codigo': program_row.get('campo_especifico_codigo'),
                'campo_especifico_nombre': program_row.get('campo_especifico_nombre'),
                'campo_detallado_codigo': program_row.get('campo_detallado_codigo'),
                'campo_detallado_nombre': program_row.get('campo_detallado_nombre'),
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
        Match all SERVIR academic profiles to CLASIFICADOR.
        
        Args:
            sample_size (int): If provided, sample this many profiles
        
        Returns:
            DataFrame with match results
        """
        # Get processing status
        status = operations.get_processing_status(
            self.cleaned_db_path,
            self.matches_db_path
        )
        
        all_profiles = status['all_profiles']
        existing_profiles = status['existing_profiles']
        profiles_to_process = status['profiles_to_process']
        
        print(f"\nLoaded {len(all_profiles)} unique academic profiles from SERVIR")
        
        if len(existing_profiles) > 0:
            print(f"Found {len(existing_profiles)} profiles already in matches DB")
        
        if len(profiles_to_process) == 0:
            print("\n✓ All profiles already in matches DB. Nothing to process!")
            return pd.DataFrame()
        
        # Apply sampling if requested
        if sample_size and sample_size < len(profiles_to_process):
            profiles_to_process = profiles_to_process.sample(n=sample_size, random_state=42)
            print(f"Sampling {sample_size} random profiles for processing...")
        
        print(f"\nProcessing {len(profiles_to_process)} NEW profiles...")
        all_results = []
        
        for _, row in tqdm(profiles_to_process.iterrows(), total=len(profiles_to_process)):
            academic_profile = row['academic_profile']
            
            # Parse to extract level and flags
            parsed_info = parse_academic_profile(academic_profile)
            
            # Match against CLASIFICADOR
            matches = self.combine_matches(academic_profile, parsed_info)
            
            # Add all matches to results
            all_results.extend(matches)
        
        # Create results dataframe
        results_df = pd.DataFrame(all_results)
        
        # Save to database
        operations.save_matches(self.matches_db_path, results_df)
        
        print(f"\n✓ Matching complete!")
        print(f"  - Processed {len(profiles_to_process)} unique profiles")
        print(f"  - Created {len(results_df)} match candidates")
        print(f"  - Saved to {self.matches_db_path}")
        
        return results_df


if __name__ == "__main__":
    # Paths
    script_dir = Path(__file__).resolve().parent
    project_root = script_dir.parent.parent.parent.parent.parent
    
    CLEANED_DB = project_root / "servir" / "data" / "cleaned" / "servir_jobs_cleaned.db"
    CLASIFICADOR_DB = project_root / "servir" / "data" / "reference" / "clasificador_carreras" / "clasificador_carreras.db"
    MATCHES_DB = project_root / "servir" / "data" / "transformed" / "academic" / "academic_matches.db"
    
    print(f"\nDatabase paths:")
    print(f"  Cleaned DB: {CLEANED_DB}")
    print(f"  CLASIFICADOR DB: {CLASIFICADOR_DB}")
    print(f"  Matches DB: {MATCHES_DB}")
    
    # Match
    matcher = AcademicMatcher(CLEANED_DB, CLASIFICADOR_DB, MATCHES_DB)
    results = matcher.match()  # Or: matcher.match(sample_size=50)