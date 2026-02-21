"""
SQL queries for job title validation database operations.

All database read/write queries are centralized here.
"""

import sqlite3
import pandas as pd
from pathlib import Path


def load_isco_codes(isco_db_path):
    """
    Load all ISCO-08 nivel 4 codes from reference database.
    
    Args:
        isco_db_path (Path): Path to isco_08_peru.db
    
    Returns:
        list of dicts with 'codigo' and 'descripcion'
    """
    conn = sqlite3.connect(isco_db_path)
    query = "SELECT codigo, descripcion FROM isco_level_4"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df.to_dict('records')


def load_cleaned_titles(cleaned_db_path):
    """
    Load all cleaned job titles from servir_jobs_cleaned.db.
    
    Args:
        cleaned_db_path (Path): Path to servir_jobs_cleaned.db
    
    Returns:
        DataFrame with 'job_title' column
    """
    conn = sqlite3.connect(cleaned_db_path)
    query = "SELECT DISTINCT job_title FROM cleaned_jobs WHERE job_title IS NOT NULL"
    df = pd.read_sql_query(query, conn)
    conn.close()
    
    return df


def get_existing_titles(validation_db_path):
    """
    Get list of job titles that already exist in validation database.
    
    Args:
        validation_db_path (Path): Path to job_title_validation.db
    
    Returns:
        set of job title strings
    """
    if not validation_db_path.exists():
        return set()
    
    conn = sqlite3.connect(validation_db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT DISTINCT job_title FROM job_title_matches")
    existing_titles = set(row[0] for row in cursor.fetchall())
    conn.close()
    
    return existing_titles


def delete_unvalidated_matches(conn, job_titles):
    """
    Delete existing unvalidated matches for given job titles.
    
    Args:
        conn: SQLite connection object
        job_titles (list): List of job titles to delete
    """
    if len(job_titles) == 0:
        return
    
    titles_tuple = tuple(job_titles)
    if len(titles_tuple) == 1:
        conn.execute(
            "DELETE FROM job_title_matches WHERE job_title = ? AND validated = 0",
            titles_tuple
        )
    else:
        placeholders = ','.join('?' * len(titles_tuple))
        conn.execute(
            f"DELETE FROM job_title_matches WHERE job_title IN ({placeholders}) AND validated = 0",
            titles_tuple
        )


def insert_match(conn, match_data):
    """
    Insert a single match result into database.
    
    Args:
        conn: SQLite connection object
        match_data (dict): Dictionary with match information
    """
    conn.execute("""
    INSERT INTO job_title_matches 
    (job_title, candidate_codigo, candidate_descripcion, semantic_confidence, 
     fuzzy_confidence, best_confidence, rank)
    VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (
        match_data['job_title'],
        match_data['candidate_codigo'],
        match_data['candidate_descripcion'],
        int(match_data['semantic_confidence']),
        int(match_data['fuzzy_confidence']),
        int(match_data['best_confidence']),
        match_data['rank']
    ))


def insert_matches_batch(conn, results_df):
    """
    Insert multiple match results into database.
    
    Args:
        conn: SQLite connection object
        results_df (DataFrame): DataFrame with match results
    """
    for _, row in results_df.iterrows():
        insert_match(conn, row.to_dict())