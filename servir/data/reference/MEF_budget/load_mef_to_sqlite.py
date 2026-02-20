"""
Load MEF budget data from CSV into SQLite database.

Creates a clean, indexed database for fast institution lookups.
"""

import sqlite3
import pandas as pd
from pathlib import Path
import sys

# Configuration
REPO_ROOT = Path("/Users/francosebastiani/Documents/GitHub/Peruvian-Civil-Service-Hiring-Patterns-Analysis")
CSV_FILE = REPO_ROOT / "servir" / "data" / "reference" / "MEF_budget" / "comparativo_gastos_2022_2025.csv"
DB_FILE = REPO_ROOT / "servir" / "data" / "reference" / "MEF_budget" / "mef_budget.db"


def load_mef_to_sqlite(csv_file, db_file, chunksize=10000):
    """
    Load MEF budget CSV into SQLite database with institution catalog extraction.
    
    Args:
        csv_file (Path): Path to comparativo_gastos_2022_2025.csv
        db_file (Path): Path to output mef_budget.db
        chunksize (int): Rows to process at once (for large files)
    """
    
    print(f"Loading MEF data from: {csv_file}")
    print(f"Target database: {db_file}")
    
    if not csv_file.exists():
        print(f"✗ CSV file not found: {csv_file}")
        sys.exit(1)
    
    # Create database
    print("\nCreating database...")
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Drop old tables
    cursor.execute("DROP TABLE IF EXISTS budget_data")
    cursor.execute("DROP TABLE IF EXISTS institutions")
    
    # Read CSV in chunks (file might be very large)
    print("\nReading CSV file...")
    
    try:
        # First, peek at the structure
        sample_df = pd.read_csv(csv_file, nrows=5)
        print(f"\nCSV columns found: {len(sample_df.columns)}")
        print("First few columns:", sample_df.columns[:10].tolist())
        
        # Load full data in chunks
        print("\nLoading full dataset (this may take a few minutes)...")
        chunk_iter = pd.read_csv(csv_file, chunksize=chunksize)
        
        total_rows = 0
        for i, chunk in enumerate(chunk_iter, 1):
            # Save to budget_data table
            chunk.to_sql('budget_data', conn, if_exists='append', index=False)
            total_rows += len(chunk)
            print(f"  Processed {total_rows:,} rows...")
        
        print(f"\n✓ Loaded {total_rows:,} total records into budget_data table")
        
    except Exception as e:
        print(f"✗ Error loading CSV: {e}")
        conn.close()
        sys.exit(1)
    
    # Create institutions catalog (unique institutions with all identifiers)
    print("\nCreating institutions catalog...")
    
    cursor.execute("""
    CREATE TABLE institutions AS
    SELECT DISTINCT
        EJECUTORA,
        EJECUTORA_NOMBRE,
        NIVEL_GOBIERNO,
        NIVEL_GOBIERNO_NOMBRE,
        SECTOR,
        SECTOR_NOMBRE,
        PLIEGO,
        PLIEGO_NOMBRE,
        SEC_EJEC,
        DEPARTAMENTO_EJECUTORA,
        DEPARTAMENTO_EJECUTORA_NOMBRE,
        PROVINCIA_EJECUTORA,
        PROVINCIA_EJECUTORA_NOMBRE,
        DISTRITO_EJECUTORA,
        DISTRITO_EJECUTORA_NOMBRE
    FROM budget_data
    WHERE EJECUTORA IS NOT NULL
    """)
    
    # Create indexes for fast lookups
    print("Creating indexes...")
    cursor.execute("CREATE INDEX idx_institutions_ejecutora ON institutions(EJECUTORA)")
    cursor.execute("CREATE INDEX idx_institutions_nombre ON institutions(EJECUTORA_NOMBRE)")
    cursor.execute("CREATE INDEX idx_budget_ejecutora ON budget_data(EJECUTORA)")
    
    conn.commit()
    
    # Print summary
    cursor.execute("SELECT COUNT(*) FROM institutions")
    institution_count = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM budget_data")
    budget_count = cursor.fetchone()[0]
    
    print(f"\n✓ Database creation complete!")
    print(f"  Total budget records: {budget_count:,}")
    print(f"  Unique institutions: {institution_count:,}")
    print(f"  Saved to: {db_file}")
    
    conn.close()


if __name__ == "__main__":
    load_mef_to_sqlite(CSV_FILE, DB_FILE)