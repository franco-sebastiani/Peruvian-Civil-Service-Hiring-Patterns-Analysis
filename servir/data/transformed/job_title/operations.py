"""
Database operations for job title validation.

High-level operations that combine queries with transaction management.
"""

import sqlite3
from pathlib import Path
from . import schema, queries


def initialize_validation_db(validation_db_path):
    """
    Create validation database and tables if they don't exist.
    
    Args:
        validation_db_path (Path): Path to job_title_validation.db
    """
    validation_db_path.parent.mkdir(parents=True, exist_ok=True)
    
    conn = sqlite3.connect(validation_db_path)
    conn.execute(schema.CREATE_JOB_TITLE_MATCHES_TABLE)
    conn.commit()
    conn.close()


def save_matches(validation_db_path, results_df):
    """
    Save match results to validation database.
    
    Handles:
    - Creating database/tables if needed
    - Deleting old unvalidated matches for these titles
    - Inserting new match results
    - Transaction management
    
    Args:
        validation_db_path (Path): Path to job_title_validation.db
        results_df (DataFrame): Match results to save
    """
    if len(results_df) == 0:
        print("No results to save.")
        return
    
    # Ensure database exists
    initialize_validation_db(validation_db_path)
    
    conn = sqlite3.connect(validation_db_path)
    
    try:
        # Delete old unvalidated matches for these titles
        job_titles = results_df['job_title'].unique().tolist()
        queries.delete_unvalidated_matches(conn, job_titles)
        
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


def get_processing_status(cleaned_db_path, validation_db_path):
    """
    Determine which titles need processing.
    
    Args:
        cleaned_db_path (Path): Path to servir_jobs_cleaned.db
        validation_db_path (Path): Path to job_title_validation.db
    
    Returns:
        dict with 'all_titles', 'existing_titles', 'titles_to_process'
    """
    # Load all cleaned titles
    all_titles_df = queries.load_cleaned_titles(cleaned_db_path)
    
    # Get titles already in validation DB
    existing_titles = queries.get_existing_titles(validation_db_path)
    
    # Filter to only new titles
    titles_to_process = all_titles_df[
        ~all_titles_df['job_title'].isin(existing_titles)
    ]
    
    return {
        'all_titles': all_titles_df,
        'existing_titles': existing_titles,
        'titles_to_process': titles_to_process
    }