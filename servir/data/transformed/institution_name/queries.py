"""
SQL queries for institution validation.

Handles:
- Loading institutions from SERVIR cleaned database
- Loading MEF institution catalog from LOCAL DATABASE (not API - UK access blocked)
- Database read/write operations
"""

import sqlite3
import pandas as pd
from pathlib import Path


def load_servir_institutions(cleaned_db_path):
    """
    Load all unique institutions from SERVIR cleaned database.
    
    Args:
        cleaned_db_path (Path): Path to servir_jobs_cleaned.db
    
    Returns:
        DataFrame with 'institution' column
    """
    conn = sqlite3.connect(cleaned_db_path)
    query = "SELECT DISTINCT institution FROM cleaned_jobs WHERE institution IS NOT NULL"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df


def load_mef_institutions(mef_db_path):
    """
    Load MEF institution catalog from local database.
    
    This replaces the API calls since API access is blocked from UK.
    Database created by load_mef_to_sqlite.py script.
    
    Args:
        mef_db_path (Path): Path to mef_budget.db
    
    Returns:
        DataFrame with all MEF institutions and identifiers
    """
    if not mef_db_path.exists():
        raise FileNotFoundError(
            f"MEF database not found: {mef_db_path}\n"
            f"Run load_mef_to_sqlite.py first to create it from CSV."
        )
    
    conn = sqlite3.connect(mef_db_path)
    
    query = """
    SELECT 
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
    FROM institutions
    ORDER BY EJECUTORA
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"Loaded {len(df)} unique MEF institutions from local database")
    
    return df


def get_existing_institutions(matches_db_path):
    """
    Get list of SERVIR institutions already in matches database.
    
    Args:
        matches_db_path (Path): Path to institution_name_matches.db
    
    Returns:
        set of institution name strings
    """
    if not matches_db_path.exists():
        return set()
    
    conn = sqlite3.connect(matches_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT servir_institution_name FROM institution_matches")
    existing = set(row[0] for row in cursor.fetchall())
    conn.close()
    
    return existing


def delete_unvalidated_matches(conn, institution_names):
    """
    Delete existing unvalidated matches for given institutions.
    
    Args:
        conn: SQLite connection object
        institution_names (list): List of institution names to delete
    """
    if len(institution_names) == 0:
        return
    
    names_tuple = tuple(institution_names)
    if len(names_tuple) == 1:
        conn.execute(
            "DELETE FROM institution_matches WHERE servir_institution_name = ? AND validated = 0",
            names_tuple
        )
    else:
        placeholders = ','.join('?' * len(names_tuple))
        conn.execute(
            f"DELETE FROM institution_matches WHERE servir_institution_name IN ({placeholders}) AND validated = 0",
            names_tuple
        )


def insert_match(conn, match_data):
    """
    Insert a single institution match result into database.
    
    Args:
        conn: SQLite connection object
        match_data (dict): Dictionary with match information
    """
    conn.execute("""
    INSERT INTO institution_matches 
    (servir_institution_name, nivel_gobierno, nivel_gobierno_nombre, sector, sector_nombre,
     pliego, pliego_nombre, ejecutora, ejecutora_nombre, sec_ejec,
     departamento_ejecutora, departamento_ejecutora_nombre,
     provincia_ejecutora, provincia_ejecutora_nombre,
     distrito_ejecutora, distrito_ejecutora_nombre,
     semantic_confidence, fuzzy_confidence, best_confidence, rank)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        match_data['servir_institution_name'],
        match_data.get('nivel_gobierno'),
        match_data.get('nivel_gobierno_nombre'),
        match_data.get('sector'),
        match_data.get('sector_nombre'),
        match_data.get('pliego'),
        match_data.get('pliego_nombre'),
        match_data['ejecutora'],
        match_data['ejecutora_nombre'],
        match_data.get('sec_ejec'),
        match_data.get('departamento_ejecutora'),
        match_data.get('departamento_ejecutora_nombre'),
        match_data.get('provincia_ejecutora'),
        match_data.get('provincia_ejecutora_nombre'),
        match_data.get('distrito_ejecutora'),
        match_data.get('distrito_ejecutora_nombre'),
        int(match_data['semantic_confidence']),
        int(match_data['fuzzy_confidence']),
        int(match_data['best_confidence']),
        match_data['rank']
    ))


def insert_matches_batch(conn, results_df):
    """
    Insert multiple institution match results into database.
    
    Args:
        conn: SQLite connection object
        results_df (DataFrame): DataFrame with match results
    """
    for _, row in results_df.iterrows():
        insert_match(conn, row.to_dict())