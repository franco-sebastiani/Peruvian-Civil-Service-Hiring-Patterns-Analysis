"""
SQL queries for academic profile matching database operations.

All database read/write queries are centralized here.
"""

import sqlite3
import pandas as pd
from pathlib import Path


def load_clasificador_programs(clasificador_db_path):
    """
    Load all programs from CLASIFICADOR database.
    
    Args:
        clasificador_db_path (Path): Path to clasificador_carreras.db
    
    Returns:
        DataFrame with all programs
    """
    if not clasificador_db_path.exists():
        raise FileNotFoundError(
            f"CLASIFICADOR database not found: {clasificador_db_path}\n"
            f"Run load_clasificador_to_sqlite.py first."
        )
    
    conn = sqlite3.connect(clasificador_db_path)
    
    query = """
    SELECT 
        programa_codigo,
        programa_nombre,
        campo_amplio_codigo,
        campo_amplio_nombre,
        campo_especifico_codigo,
        campo_especifico_nombre,
        campo_detallado_codigo,
        campo_detallado_nombre,
        academic_level
    FROM programs
    WHERE programa_codigo IS NOT NULL
    """
    
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    print(f"Loaded {len(df)} programs from CLASIFICADOR")
    
    return df


def load_servir_academic_profiles(cleaned_db_path):
    """
    Load all unique academic profiles from SERVIR cleaned database.
    
    Args:
        cleaned_db_path (Path): Path to servir_jobs_cleaned.db
    
    Returns:
        DataFrame with 'academic_profile' column
    """
    conn = sqlite3.connect(cleaned_db_path)
    query = "SELECT DISTINCT academic_profile FROM cleaned_jobs WHERE academic_profile IS NOT NULL"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df


def get_existing_profiles(matches_db_path):
    """
    Get list of academic profiles already in matches database.
    
    Args:
        matches_db_path (Path): Path to academic_matches.db
    
    Returns:
        set of academic profile strings
    """
    if not matches_db_path.exists():
        return set()
    
    conn = sqlite3.connect(matches_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT servir_academic_profile FROM academic_matches")
    existing = set(row[0] for row in cursor.fetchall())
    conn.close()
    
    return existing


def delete_unvalidated_matches(conn, academic_profiles):
    """
    Delete existing unvalidated matches for given profiles.
    
    Args:
        conn: SQLite connection object
        academic_profiles (list): List of academic profiles to delete
    """
    if len(academic_profiles) == 0:
        return
    
    profiles_tuple = tuple(academic_profiles)
    if len(profiles_tuple) == 1:
        conn.execute(
            "DELETE FROM academic_matches WHERE servir_academic_profile = ? AND validated = 0",
            profiles_tuple
        )
    else:
        placeholders = ','.join('?' * len(profiles_tuple))
        conn.execute(
            f"DELETE FROM academic_matches WHERE servir_academic_profile IN ({placeholders}) AND validated = 0",
            profiles_tuple
        )


def insert_match(conn, match_data):
    """
    Insert a single academic match result into database.
    
    Args:
        conn: SQLite connection object
        match_data (dict): Dictionary with match information
    """
    conn.execute("""
    INSERT INTO academic_matches 
    (servir_academic_profile, academic_level_code, thesis_required,
     accepts_related_fields, requires_colegiado, requires_habilitado, multiple_options_allowed,
     programa_codigo, programa_nombre,
     campo_amplio_codigo, campo_amplio_nombre,
     campo_especifico_codigo, campo_especifico_nombre,
     campo_detallado_codigo, campo_detallado_nombre,
     semantic_confidence, fuzzy_confidence, best_confidence, rank)
    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, (
        match_data['servir_academic_profile'],
        match_data.get('academic_level_code'),
        match_data.get('thesis_required', 0),
        match_data.get('accepts_related_fields', 0),
        match_data.get('requires_colegiado', 0),
        match_data.get('requires_habilitado', 0),
        match_data.get('multiple_options_allowed', 0),
        match_data['programa_codigo'],
        match_data['programa_nombre'],
        match_data.get('campo_amplio_codigo'),
        match_data.get('campo_amplio_nombre'),
        match_data.get('campo_especifico_codigo'),
        match_data.get('campo_especifico_nombre'),
        match_data.get('campo_detallado_codigo'),
        match_data.get('campo_detallado_nombre'),
        int(match_data['semantic_confidence']),
        int(match_data['fuzzy_confidence']),
        int(match_data['best_confidence']),
        match_data['rank']
    ))


def insert_matches_batch(conn, results_df):
    """
    Insert multiple academic match results into database.
    
    Args:
        conn: SQLite connection object
        results_df (DataFrame): DataFrame with match results
    """
    for _, row in results_df.iterrows():
        insert_match(conn, row.to_dict())