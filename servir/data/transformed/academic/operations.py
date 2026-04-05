"""
Database operations for academic profile matching.

High-level operations that combine queries with transaction management.
"""

import sqlite3
from pathlib import Path
from . import schema, queries


def initialize_matches_db(matches_db_path):
    """
    Create matches database and tables if they don't exist.
    
    Args:
        matches_db_path (Path): Path to academic_matches.db
    """
    matches_db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(matches_db_path)
    conn.execute(schema.CREATE_ACADEMIC_MATCHES_TABLE)
    conn.commit()
    conn.close()


def save_matches(matches_db_path, results_df):
    """
    Save academic match results to matches database.
    
    Handles:
    - Creating database/tables if needed
    - Deleting old unvalidated matches
    - Inserting new match results
    - Transaction management
    
    Args:
        matches_db_path (Path): Path to academic_matches.db
        results_df (DataFrame): Match results to save
    """
    if len(results_df) == 0:
        print("No results to save.")
        return
    
    # Ensure database exists
    initialize_matches_db(matches_db_path)
    
    conn = sqlite3.connect(matches_db_path)
    
    try:
        # Delete old unvalidated matches
        academic_profiles = results_df['servir_academic_profile'].unique().tolist()
        queries.delete_unvalidated_matches(conn, academic_profiles)
        
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
    Determine which academic profiles need processing.
    
    Args:
        cleaned_db_path (Path): Path to servir_jobs_cleaned.db
        matches_db_path (Path): Path to academic_matches.db
    
    Returns:
        dict with processing status
    """
    # Load all SERVIR academic profiles
    all_profiles_df = queries.load_servir_academic_profiles(cleaned_db_path)
    
    # Get profiles already in matches DB
    existing_profiles = queries.get_existing_profiles(matches_db_path)
    
    # Filter to only new profiles
    profiles_to_process = all_profiles_df[
        ~all_profiles_df['academic_profile'].isin(existing_profiles)
    ]
    
    return {
        'all_profiles': all_profiles_df,
        'existing_profiles': existing_profiles,
        'profiles_to_process': profiles_to_process
    }


def load_clasificador_catalog(clasificador_db_path):
    """
    Load CLASIFICADOR program catalog.
    
    Args:
        clasificador_db_path (Path): Path to clasificador_carreras.db
    
    Returns:
        DataFrame with CLASIFICADOR programs
    """
    return queries.load_clasificador_programs(clasificador_db_path)