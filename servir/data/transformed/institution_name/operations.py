"""
Database operations for institution name matching.

High-level operations that combine queries with transaction management.
"""

import sqlite3
from pathlib import Path
from . import schema, queries


def initialize_matches_db(matches_db_path):
    """
    Create matches database and tables if they don't exist.
    
    Args:
        matches_db_path (Path): Path to institution_name_matches.db
    """
    matches_db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(matches_db_path)
    conn.execute(schema.CREATE_INSTITUTION_MATCHES_TABLE)
    conn.commit()
    conn.close()


def save_matches(matches_db_path, results_df):
    """
    Save institution match results to matches database.
    
    Handles:
    - Creating database/tables if needed
    - Deleting old unvalidated matches for these institutions
    - Inserting new match results
    - Transaction management
    
    Args:
        matches_db_path (Path): Path to institution_name_matches.db
        results_df (DataFrame): Match results to save
    """
    if len(results_df) == 0:
        print("No results to save.")
        return
    
    # Ensure database exists
    initialize_matches_db(matches_db_path)
    
    conn = sqlite3.connect(matches_db_path)
    
    try:
        # Delete old unvalidated matches for these institutions
        institution_names = results_df['servir_institution_name'].unique().tolist()
        queries.delete_unvalidated_matches(conn, institution_names)
        
        # Insert new matches
        queries.insert_matches_batch(conn, results_df)
        
        # Commit transaction
        conn.commit()
        print(f"✓ Saved {len(results_df)} match results to database")
        
    except Exception as e:
        conn.rollback()
        print(f"Error saving to database: {e}")
        raise
    finally:
        conn.close()


def get_processing_status(cleaned_db_path, matches_db_path):
    """
    Determine which institutions need processing.
    
    Args:
        cleaned_db_path (Path): Path to servir_jobs_cleaned.db
        matches_db_path (Path): Path to institution_name_matches.db
    
    Returns:
        dict with 'all_institutions', 'existing_institutions', 'institutions_to_process'
    """
    # Load all SERVIR institutions
    all_institutions_df = queries.load_servir_institutions(cleaned_db_path)
    
    # Get institutions already in matches DB
    existing_institutions = queries.get_existing_institutions(matches_db_path)
    
    # Filter to only new institutions
    institutions_to_process = all_institutions_df[
        ~all_institutions_df['institution'].isin(existing_institutions)
    ]
    
    return {
        'all_institutions': all_institutions_df,
        'existing_institutions': existing_institutions,
        'institutions_to_process': institutions_to_process
    }


def fetch_and_cache_mef_catalog(mef_db_path):
    """
    Load MEF institution catalog from local database.
    
    Replaces API fetching since API access is blocked from UK.
    
    Args:
        mef_db_path (Path): Path to mef_budget.db
    
    Returns:
        DataFrame with MEF institutions
    """
    return queries.load_mef_institutions(mef_db_path)