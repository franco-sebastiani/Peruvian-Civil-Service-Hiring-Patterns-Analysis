"""
Orchestrator - Manages the complete transformation pipeline.

Coordinates:
1. Loading data from servir_jobs_cleaned.db
2. Fitting LDA models on full dataset (knowledge, competencies, specialization)
3. Transforming each observation using job_transformer
4. Saving to servir_jobs_transformed.db

Handles all database I/O and iteration.
"""

import sqlite3
import sys
from pathlib import Path
import pandas as pd
from tqdm import tqdm

# Setup paths
project_root = Path(__file__).resolve().parent.parent.parent.parent
sys.path.insert(0, str(project_root))

from servir.src.transforming.pipeline.job_transformer import JobTransformer
from servir.src.transforming.transformers.knowledge.knowledge_transformer import KnowledgeTransformer
from servir.src.transforming.transformers.competencies.competencies_transformer import CompetenciesTransformer
from servir.src.transforming.transformers.specialization.specialization_transformer import SpecializationTransformer


class TransformationOrchestrator:
    """Orchestrate the complete transformation pipeline."""
    
    def __init__(self, cleaned_db_path, transformed_db_path, matches_dbs):
        """
        Initialize orchestrator.
        
        Args:
            cleaned_db_path (Path): Path to servir_jobs_cleaned.db
            transformed_db_path (Path): Path to servir_jobs_transformed.db (output)
            matches_dbs (dict): Paths to all match databases
        """
        self.cleaned_db_path = Path(cleaned_db_path)
        self.transformed_db_path = Path(transformed_db_path)
        self.matches_dbs = matches_dbs
        
        # Will be fitted during run
        self.knowledge_lda = None
        self.competencies_lda = None
        self.specialization_lda = None
    
    def run(self):
        """
        Run the complete transformation pipeline.
        
        Process:
        1. Load cleaned jobs
        2. Fit LDA models on full dataset
        3. Transform each job
        4. Save to transformed database
        """
        print("=" * 80)
        print("TRANSFORMATION PIPELINE")
        print("=" * 80)
        
        # Step 1: Load cleaned data
        print("\n[Step 1] Loading cleaned jobs...")
        conn_cleaned = sqlite3.connect(self.cleaned_db_path)
        cleaned_df = pd.read_sql_query("SELECT * FROM cleaned_jobs", conn_cleaned)
        conn_cleaned.close()
        
        print(f"✓ Loaded {len(cleaned_df)} jobs from cleaned database")
        
        # Step 2: Fit LDA models on full dataset
        print("\n[Step 2] Fitting LDA models...")
        
        print("\n  Fitting Knowledge LDA...")
        self.knowledge_lda = KnowledgeTransformer(n_topics=15)
        knowledge_topics_df = self.knowledge_lda.fit_transform(cleaned_df['knowledge'].tolist())
        
        print("\n  Fitting Competencies LDA...")
        self.competencies_lda = CompetenciesTransformer(n_topics=15)
        competencies_topics_df = self.competencies_lda.fit_transform(cleaned_df['competencies'].tolist())
        
        print("\n  Fitting Specialization LDA...")
        self.specialization_lda = SpecializationTransformer(n_topics=15)
        specialization_topics_df = self.specialization_lda.fit_transform(cleaned_df['specialization'].tolist())
        
        print("\n✓ All LDA models fitted")
        
        # Print discovered topics for review
        print("\n" + "=" * 80)
        print("Discovered Topics (top 10 words per topic)")
        print("=" * 80)
        
        print("\nKnowledge Topics:")
        for topic, words in self.knowledge_lda.get_top_words_per_topic(10).items():
            print(f"  {topic}: {', '.join(words)}")
        
        print("\nCompetencies Topics:")
        for topic, words in self.competencies_lda.get_top_words_per_topic(10).items():
            print(f"  {topic}: {', '.join(words)}")
        
        print("\nSpecialization Topics:")
        for topic, words in self.specialization_lda.get_top_words_per_topic(10).items():
            print(f"  {topic}: {', '.join(words)}")
        
        # Step 3: Initialize job transformer
        print("\n[Step 3] Initializing job transformer...")
        job_transformer = JobTransformer(
            job_title_matches_db=self.matches_dbs['job_title'],
            institution_matches_db=self.matches_dbs['institution_name'],
            academic_matches_db=self.matches_dbs['academic']
        )
        
        # Step 4: Transform each job
        print("\n[Step 4] Transforming all jobs...")
        transformed_rows = []
        
        for idx, row in tqdm(cleaned_df.iterrows(), total=len(cleaned_df), desc="Transforming"):
            # Transform using job_transformer (gets most columns)
            transformed_row = job_transformer.transform(row.to_dict())
            
            # Add LDA topic distributions
            for col in knowledge_topics_df.columns:
                transformed_row[col] = knowledge_topics_df.iloc[idx][col]
            
            for col in competencies_topics_df.columns:
                transformed_row[col] = competencies_topics_df.iloc[idx][col]
            
            for col in specialization_topics_df.columns:
                transformed_row[col] = specialization_topics_df.iloc[idx][col]
            
            transformed_rows.append(transformed_row)
        
        # Convert to DataFrame
        transformed_df = pd.DataFrame(transformed_rows)
        
        print(f"\n✓ Transformed {len(transformed_df)} jobs")
        
        # Step 5: Save to transformed database
        print("\n[Step 5] Saving to transformed database...")
        self.transformed_db_path.parent.mkdir(parents=True, exist_ok=True)
        
        conn_transformed = sqlite3.connect(self.transformed_db_path)
        transformed_df.to_sql('transformed_jobs', conn_transformed, if_exists='replace', index=False)
        
        # Create indexes
        print("Creating indexes...")
        cursor = conn_transformed.cursor()
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_isco_code ON transformed_jobs(isco_code)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_ejecutora ON transformed_jobs(ejecutora)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_programa_codigo ON transformed_jobs(programa_codigo)")
        cursor.execute("CREATE INDEX IF NOT EXISTS idx_posting_id ON transformed_jobs(posting_unique_id)")
        
        conn_transformed.commit()
        
        # Print summary
        print("\n" + "=" * 80)
        print("TRANSFORMATION COMPLETE")
        print("=" * 80)
        
        cursor.execute("SELECT COUNT(*) FROM transformed_jobs")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM transformed_jobs WHERE isco_code IS NOT NULL")
        with_isco = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM transformed_jobs WHERE ejecutora IS NOT NULL")
        with_mef = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM transformed_jobs WHERE programa_codigo IS NOT NULL")
        with_program = cursor.fetchone()[0]
        
        print(f"\nTotal jobs: {total:,}")
        print(f"Jobs with ISCO code: {with_isco:,} ({with_isco/total*100:.1f}%)")
        print(f"Jobs with MEF institution: {with_mef:,} ({with_mef/total*100:.1f}%)")
        print(f"Jobs with academic program: {with_program:,} ({with_program/total*100:.1f}%)")
        
        print(f"\n✓ Transformed database saved to: {self.transformed_db_path}")
        print(f"\nOutput columns: {len(transformed_df.columns)}")
        print(f"  Original: ~14 columns")
        print(f"  Added: ~{len(transformed_df.columns) - 14} transformed columns")
        
        conn_transformed.close()
        
        return transformed_df


if __name__ == "__main__":
    # Paths
    CLEANED_DB = project_root / "servir" / "data" / "cleaned" / "servir_jobs_cleaned.db"
    TRANSFORMED_DB = project_root / "servir" / "data" / "transformed" / "servir_jobs_transformed.db"
    
    MATCHES_DBS = {
        'job_title': project_root / "servir" / "data" / "transformed" / "job_title" / "job_title_matches.db",
        'institution_name': project_root / "servir" / "data" / "transformed" / "institution_name" / "institution_name_matches.db",
        'academic': project_root / "servir" / "data" / "transformed" / "academic" / "academic_matches.db"
    }
    
    print("Checking match databases...")
    for name, path in MATCHES_DBS.items():
        print(f"  {name}: {path.exists()}")
        if not path.exists():
            print(f"\n✗ Missing: {path}")
            print(f"Run the {name}_matcher.py first!")
            sys.exit(1)
    
    print("\n✓ All match databases found")
    
    # Run orchestrator
    orchestrator = TransformationOrchestrator(CLEANED_DB, TRANSFORMED_DB, MATCHES_DBS)
    orchestrator.run()