import sqlite3
import pandas as pd
from pathlib import Path

# Configuration - Relative paths from script location
SCRIPT_DIR = Path(__file__).parent
CSV_FILE = SCRIPT_DIR / "../../data/reference/isco_08_peru.csv"
DB_FILE = SCRIPT_DIR / "../../data/reference/isco_08_peru.db"

def load_isco_to_sqlite(csv_file, db_file):
    """
    Load ISCO-08 Peru CSV into normalized SQLite database with 4-level hierarchy.
    
    Steps:
    1. Read CSV and extract unique (code, description) pairs for each level
    2. Create 4 normalized tables (gran_grupos, sub_grupos_principal, sub_grupos, grupos_primarios)
    3. Build foreign key relationships between levels
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
    cursor.execute("DROP TABLE IF EXISTS grupos_primarios")
    cursor.execute("DROP TABLE IF EXISTS sub_grupos")
    cursor.execute("DROP TABLE IF EXISTS sub_grupos_principal")
    cursor.execute("DROP TABLE IF EXISTS gran_grupos")
    
    # Disable foreign key checks during insertion (re-enable after)
    cursor.execute("PRAGMA foreign_keys = OFF")
    
    # Create Level 1: Gran grupos
    print("Creating gran_grupos table...")
    cursor.execute("""
    CREATE TABLE gran_grupos (
        id INTEGER PRIMARY KEY,
        codigo TEXT UNIQUE NOT NULL,
        descripcion TEXT NOT NULL
    )
    """)
    
    # Ensure all codes are stored as TEXT to preserve leading zeros
    cursor.execute("PRAGMA foreign_keys = OFF")
    
    # Extract unique Level 1 items
    level1 = df[['Gran grupo', 'Descripcion CNO 2015']].dropna(subset=['Gran grupo']).drop_duplicates()
    level1 = level1[level1['Gran grupo'].notna()].reset_index(drop=True)
    
    for _, row in level1.iterrows():
        cursor.execute(
            "INSERT INTO gran_grupos (codigo, descripcion) VALUES (?, ?)",
            (str(row['Gran grupo']).strip(), str(row['Descripcion CNO 2015']).strip())
        )
    print(f"  Inserted {len(level1)} gran_grupos")
    
    # Create Level 2: Sub grupos principal
    print("Creating sub_grupos_principal table...")
    cursor.execute("""
    CREATE TABLE sub_grupos_principal (
        id INTEGER PRIMARY KEY,
        codigo TEXT UNIQUE NOT NULL,
        gran_grupo_codigo TEXT NOT NULL,
        descripcion TEXT NOT NULL,
        FOREIGN KEY(gran_grupo_codigo) REFERENCES gran_grupos(codigo)
    )
    """)
    
    # Extract unique Level 2 items
    level2 = df[df['Sub grupo principal'].notna()][['Gran grupo', 'Sub grupo principal', 'Descripcion CNO 2015']].drop_duplicates()
    level2 = level2.reset_index(drop=True)
    
    for _, row in level2.iterrows():
        code = str(row['Sub grupo principal']).strip()
        # Extract parent: for "11", parent is "1"; for "01", parent is "0"
        parent_code = code[0]  # Just take first character
        cursor.execute(
            "INSERT INTO sub_grupos_principal (codigo, gran_grupo_codigo, descripcion) VALUES (?, ?, ?)",
            (code, parent_code, str(row['Descripcion CNO 2015']).strip())
        )
    print(f"  Inserted {len(level2)} sub_grupos_principal")
    
    # Create Level 3: Sub grupos
    print("Creating sub_grupos table...")
    cursor.execute("""
    CREATE TABLE sub_grupos (
        id INTEGER PRIMARY KEY,
        codigo TEXT UNIQUE NOT NULL,
        sub_grupo_principal_codigo TEXT NOT NULL,
        descripcion TEXT NOT NULL,
        FOREIGN KEY(sub_grupo_principal_codigo) REFERENCES sub_grupos_principal(codigo)
    )
    """)
    
    # Extract unique Level 3 items
    level3 = df[df['Sub grupo'].notna()][['Sub grupo principal', 'Sub grupo', 'Descripcion CNO 2015']].drop_duplicates()
    level3 = level3.reset_index(drop=True)
    
    for _, row in level3.iterrows():
        code = str(row['Sub grupo']).strip()
        parent_code = code[:len(code)-1]  # "111" -> "11"
        cursor.execute(
            "INSERT INTO sub_grupos (codigo, sub_grupo_principal_codigo, descripcion) VALUES (?, ?, ?)",
            (code, parent_code, str(row['Descripcion CNO 2015']).strip())
        )
    print(f"  Inserted {len(level3)} sub_grupos")
    
    # Create Level 4: Grupos primarios
    print("Creating grupos_primarios table...")
    cursor.execute("""
    CREATE TABLE grupos_primarios (
        id INTEGER PRIMARY KEY,
        codigo TEXT UNIQUE NOT NULL,
        sub_grupo_codigo TEXT NOT NULL,
        descripcion TEXT NOT NULL,
        FOREIGN KEY(sub_grupo_codigo) REFERENCES sub_grupos(codigo)
    )
    """)
    
    # Extract unique Level 4 items
    level4 = df[df['Grupo primario'].notna()][['Sub grupo', 'Grupo primario', 'Descripcion CNO 2015']].drop_duplicates()
    level4 = level4.reset_index(drop=True)
    
    for _, row in level4.iterrows():
        code = str(row['Grupo primario']).strip()
        parent_code = code[:len(code)-1]  # "1111" -> "111"
        cursor.execute(
            "INSERT INTO grupos_primarios (codigo, sub_grupo_codigo, descripcion) VALUES (?, ?, ?)",
            (code, parent_code, str(row['Descripcion CNO 2015']).strip())
        )
    print(f"  Inserted {len(level4)} grupos_primarios")
    
    # Commit and close
    conn.commit()
    cursor.execute("PRAGMA foreign_keys = ON")
    print("\nDatabase creation complete!")
    
    # Print summary
    cursor.execute("SELECT COUNT(*) FROM gran_grupos")
    print(f"  Gran grupos: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM sub_grupos_principal")
    print(f"  Sub grupos principal: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM sub_grupos")
    print(f"  Sub grupos: {cursor.fetchone()[0]}")
    cursor.execute("SELECT COUNT(*) FROM grupos_primarios")
    print(f"  Grupos primarios: {cursor.fetchone()[0]}")
    
    conn.close()
    print(f"\nDatabase saved to {db_file}")

if __name__ == "__main__":
    load_isco_to_sqlite(CSV_FILE, DB_FILE)