import sqlite3
import pandas as pd
from pathlib import Path

# Configuration - Paths
SCRIPT_DIR = Path(__file__).parent
CSV_FILE = SCRIPT_DIR / "isco_08_peru.csv"
DB_FILE = SCRIPT_DIR / "isco_08_peru.db"


def load_isco_to_sqlite(csv_file, db_file):
    """
    Load ISCO-08 Peru CSV into normalized SQLite database with 4-level hierarchy.
    
    Steps:
    1. Read CSV and extract unique (code, description) pairs for each level
    2. Create 4 normalized tables (isco_level_1, isco_level_2, isco_level_3, isco_level_4)
    3. Build foreign key relationships between levels
    
    Table mapping:
    - isco_level_1 = Gran grupos (broadest)
    - isco_level_2 = Sub grupos principal
    - isco_level_3 = Sub grupos
    - isco_level_4 = Grupos primarios (most specific)
    """
    
    print(f"Reading {csv_file}...")
    # Force all code columns to be read as strings to preserve leading zeros
    df = pd.read_csv(csv_file, dtype={
        'Gran grupo': str,
        'Sub grupo principal': str,
        'Sub grupo': str,
        'Grupo primario': str
    })
    
    # Create/connect to database
    print(f"Creating database {db_file}...")
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Drop existing tables (for clean reload)
    cursor.execute("DROP TABLE IF EXISTS isco_level_4")
    cursor.execute("DROP TABLE IF EXISTS isco_level_3")
    cursor.execute("DROP TABLE IF EXISTS isco_level_2")
    cursor.execute("DROP TABLE IF EXISTS isco_level_1")
    
    # Disable foreign key checks during insertion (re-enable after)
    cursor.execute("PRAGMA foreign_keys = OFF")
    
    # Create Level 1: Gran grupos
    print("Creating isco_level_1 table (Gran grupos)...")
    cursor.execute("""
    CREATE TABLE isco_level_1 (
        id INTEGER PRIMARY KEY,
        codigo TEXT UNIQUE NOT NULL,
        descripcion TEXT NOT NULL
    )
    """)
    
    # Extract unique Level 1 items
    level1 = df[['Gran grupo', 'Descripcion CNO 2015']].dropna(subset=['Gran grupo']).drop_duplicates()
    level1 = level1[level1['Gran grupo'].notna()].reset_index(drop=True)
    
    for _, row in level1.iterrows():
        cursor.execute(
            "INSERT INTO isco_level_1 (codigo, descripcion) VALUES (?, ?)",
            (str(row['Gran grupo']).strip(), str(row['Descripcion CNO 2015']).strip())
        )
    print(f"  Inserted {len(level1)} records")
    
    # Create Level 2: Sub grupos principal
    print("Creating isco_level_2 table (Sub grupos principal)...")
    cursor.execute("""
    CREATE TABLE isco_level_2 (
        id INTEGER PRIMARY KEY,
        codigo TEXT UNIQUE NOT NULL,
        parent_codigo TEXT NOT NULL,
        descripcion TEXT NOT NULL,
        FOREIGN KEY(parent_codigo) REFERENCES isco_level_1(codigo)
    )
    """)
    
    # Extract unique Level 2 items
    level2 = df[df['Sub grupo principal'].notna()][['Gran grupo', 'Sub grupo principal', 'Descripcion CNO 2015']].drop_duplicates()
    level2 = level2.reset_index(drop=True)
    
    for _, row in level2.iterrows():
        code = str(row['Sub grupo principal']).strip()
        parent_code = code[0]  # First character
        cursor.execute(
            "INSERT INTO isco_level_2 (codigo, parent_codigo, descripcion) VALUES (?, ?, ?)",
            (code, parent_code, str(row['Descripcion CNO 2015']).strip())
        )
    print(f"  Inserted {len(level2)} records")
    
    # Create Level 3: Sub grupos
    print("Creating isco_level_3 table (Sub grupos)...")
    cursor.execute("""
    CREATE TABLE isco_level_3 (
        id INTEGER PRIMARY KEY,
        codigo TEXT UNIQUE NOT NULL,
        parent_codigo TEXT NOT NULL,
        descripcion TEXT NOT NULL,
        FOREIGN KEY(parent_codigo) REFERENCES isco_level_2(codigo)
    )
    """)
    
    # Extract unique Level 3 items
    level3 = df[df['Sub grupo'].notna()][['Sub grupo principal', 'Sub grupo', 'Descripcion CNO 2015']].drop_duplicates()
    level3 = level3.reset_index(drop=True)
    
    for _, row in level3.iterrows():
        code = str(row['Sub grupo']).strip()
        parent_code = code[:len(code)-1]  # "111" -> "11"
        cursor.execute(
            "INSERT INTO isco_level_3 (codigo, parent_codigo, descripcion) VALUES (?, ?, ?)",
            (code, parent_code, str(row['Descripcion CNO 2015']).strip())
        )
    print(f"  Inserted {len(level3)} records")
    
    # Create Level 4: Grupos primarios (most specific level)
    print("Creating isco_level_4 table (Grupos primarios - most specific)...")
    cursor.execute("""
    CREATE TABLE isco_level_4 (
        id INTEGER PRIMARY KEY,
        codigo TEXT UNIQUE NOT NULL,
        parent_codigo TEXT NOT NULL,
        descripcion TEXT NOT NULL,
        FOREIGN KEY(parent_codigo) REFERENCES isco_level_3(codigo)
    )
    """)
    
    # Extract unique Level 4 items
    level4 = df[df['Grupo primario'].notna()][['Sub grupo', 'Grupo primario', 'Descripcion CNO 2015']].drop_duplicates()
    level4 = level4.reset_index(drop=True)
    
    for _, row in level4.iterrows():
        code = str(row['Grupo primario']).strip()
        parent_code = code[:len(code)-1]  # "1111" -> "111"
        cursor.execute(
            "INSERT INTO isco_level_4 (codigo, parent_codigo, descripcion) VALUES (?, ?, ?)",
            (code, parent_code, str(row['Descripcion CNO 2015']).strip())
        )
    print(f"  Inserted {len(level4)} records")
    
    # Commit and close
    conn.commit()
    cursor.execute("PRAGMA foreign_keys = ON")
    print("\nDatabase creation complete!")
    
    # Print summary
    cursor.execute("SELECT COUNT(*) FROM isco_level_1")
    print(f"  Level 1 (Gran grupos): {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM isco_level_2")
    print(f"  Level 2 (Sub grupos principal): {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM isco_level_3")
    print(f"  Level 3 (Sub grupos): {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM isco_level_4")
    print(f"  Level 4 (Grupos primarios): {cursor.fetchone()[0]}")
    
    conn.close()
    print(f"\nDatabase saved to {db_file}")


if __name__ == "__main__":
    load_isco_to_sqlite(CSV_FILE, DB_FILE)