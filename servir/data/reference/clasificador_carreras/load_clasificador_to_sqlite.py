"""
Load CLASIFICADOR DE CARRERAS from Excel into SQLite database.

Reads prog_educ_superior.xlsx (sheets 2-19) and creates a normalized database
with separate tables for each academic level.
"""

import pandas as pd
import sqlite3
from pathlib import Path
import sys

# Configuration
REPO_ROOT = Path("/Users/francosebastiani/Documents/GitHub/Peruvian-Civil-Service-Hiring-Patterns-Analysis")
EXCEL_FILE = REPO_ROOT / "servir" / "data" / "reference" / "clasificador_carreras" / "prog_educ_superior.xlsx"
DB_FILE = REPO_ROOT / "servir" / "data" / "reference" / "clasificador_carreras" / "clasificador_carreras.db"


def load_clasificador_to_sqlite(excel_file, db_file):
    """
    Load CLASIFICADOR Excel file into ONE unified SQLite table.
    
    Handles Excel formatting issues:
    - Merged cells in headers
    - Multiple header rows
    - Sparse hierarchy (forward-fill NULLs)
    - Combines all sheets into one programs table
    
    Process:
    1. Read sheets 2-19 from Excel
    2. Skip header rows, assign column names
    3. Forward-fill hierarchy columns
    4. Extract academic level from 9-digit code
    5. Combine all sheets into ONE programs table
    
    Args:
        excel_file (Path): Path to prog_educ_superior.xlsx
        db_file (Path): Path to output clasificador_carreras.db
    """
    
    print(f"Loading CLASIFICADOR data from: {excel_file}")
    print(f"Target database: {db_file}")
    
    if not excel_file.exists():
        print(f"✗ Excel file not found: {excel_file}")
        sys.exit(1)
    
    # Create database
    print("\nCreating database...")
    db_file.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_file)
    cursor = conn.cursor()
    
    # Drop old table
    cursor.execute("DROP TABLE IF EXISTS programs")
    
    # Read all sheet names
    print("\nReading Excel file structure...")
    xl_file = pd.ExcelFile(excel_file)
    all_sheets = xl_file.sheet_names
    
    print(f"Total sheets found: {len(all_sheets)}")
    
    # Process sheets 2-19 (indices 1-18 in Python)
    program_sheets = all_sheets[1:19]
    
    print(f"\nProcessing {len(program_sheets)} program sheets...")
    
    # Column names based on your structure
    column_names = [
        'campo_amplio_codigo',
        'campo_amplio_nombre',
        'campo_especifico_codigo',
        'campo_especifico_nombre',
        'campo_detallado_codigo',
        'campo_detallado_nombre',
        'programa_codigo',
        'programa_nombre',
        'codigo_campo_programa',
        'codigo_campo_programa_nivel'
    ]
    
    all_programs = []  # Collect all programs here
    
    for idx, sheet_name in enumerate(program_sheets, start=2):
        print(f"\n[Sheet {idx}] Processing: {sheet_name}")
        
        try:
            # Read sheet, skip header rows
            df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None, skiprows=4)
            
            # Assign column names
            if len(df.columns) >= len(column_names):
                df.columns = column_names + [f'extra_{i}' for i in range(len(df.columns) - len(column_names))]
            else:
                df.columns = column_names[:len(df.columns)]
            
            # Remove completely empty rows
            df = df.dropna(how='all')
            
            # Forward-fill hierarchy columns (handle sparse/merged cells)
            hierarchy_cols = [
                'campo_amplio_codigo',
                'campo_amplio_nombre',
                'campo_especifico_codigo',
                'campo_especifico_nombre'
            ]
            
            for col in hierarchy_cols:
                if col in df.columns:
                    df[col] = df[col].ffill()
            
            # Keep only rows with actual program codes
            if 'programa_codigo' in df.columns:
                df = df[df['programa_codigo'].notna()]
            
            print(f"  Programs found: {len(df)}")
            
            if len(df) == 0:
                print(f"  ⚠ No programs found, skipping...")
                continue
            
            # Extract academic level from 9-digit code (last digit)
            if 'codigo_campo_programa_nivel' in df.columns:
                # Convert to string and get last character
                df['academic_level'] = df['codigo_campo_programa_nivel'].astype(str).str[-1]
                
                # Filter out non-numeric levels (header remnants like 'l', 'n')
                df['academic_level'] = pd.to_numeric(df['academic_level'], errors='coerce')
                
                # Remove rows where academic_level is not a valid number (3-9)
                df = df[df['academic_level'].notna()]
                df = df[(df['academic_level'] >= 3) & (df['academic_level'] <= 9)]
                
                # Convert to int
                df['academic_level'] = df['academic_level'].astype(int)
            else:
                df['academic_level'] = None
            
            # Add sheet reference for debugging
            df['source_sheet'] = sheet_name
            df['source_sheet_index'] = idx
            
            # Append to collection
            all_programs.append(df)
            
            print(f"  Sample: {df.iloc[0]['programa_nombre']}")
            print(f"  Level: {df.iloc[0].get('academic_level', 'N/A')}")
            
        except Exception as e:
            print(f"  ✗ Error processing sheet: {e}")
            import traceback
            traceback.print_exc()
            continue
    
    # Combine all sheets into one DataFrame
    print("\n" + "=" * 80)
    print("Combining all sheets...")
    
    if len(all_programs) == 0:
        print("✗ No programs loaded from any sheet!")
        conn.close()
        sys.exit(1)
    
    combined_df = pd.concat(all_programs, ignore_index=True)
    
    print(f"✓ Combined {len(combined_df):,} programs from {len(all_programs)} sheets")
    
    # Save to single table
    print("\nSaving to database...")
    combined_df.to_sql('programs', conn, if_exists='replace', index=False)
    
    # Create indexes
    print("Creating indexes...")
    cursor.execute("CREATE INDEX idx_programa_codigo ON programs(programa_codigo)")
    cursor.execute("CREATE INDEX idx_codigo_campo_programa ON programs(codigo_campo_programa)")
    cursor.execute("CREATE INDEX idx_academic_level ON programs(academic_level)")
    cursor.execute("CREATE INDEX idx_campo_detallado ON programs(campo_detallado_codigo)")
    
    conn.commit()
    
    # Print summary
    print("\n" + "=" * 80)
    print("Database Summary")
    print("=" * 80)
    
    cursor.execute("SELECT COUNT(*) FROM programs")
    total = cursor.fetchone()[0]
    
    cursor.execute("SELECT academic_level, COUNT(*) FROM programs GROUP BY academic_level ORDER BY academic_level")
    level_counts = cursor.fetchall()
    
    print(f"\nTotal programs: {total:,}")
    print("\nPrograms by academic level:")
    for level, count in level_counts:
        print(f"  Level {level}: {count:,} programs")
    
    print(f"\n✓ Database creation complete!")
    print(f"  Saved to: {db_file}")
    
    conn.close()


if __name__ == "__main__":
    load_clasificador_to_sqlite(EXCEL_FILE, DB_FILE)