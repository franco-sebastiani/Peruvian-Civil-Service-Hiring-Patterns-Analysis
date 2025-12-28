"""
Database query operations for cleaning phase.

Handles reading from extracting database and checking cleaning database status.
"""

import sqlite3
from servir.src.database.connection import get_connection, close_connection


def get_all_extracted_jobs():
    """
    Get all raw jobs from the extracting database.
    
    Reads from extraction phase database to get jobs for cleaning.
    
    Returns:
        list[dict]: List of raw job dictionaries, or empty list if error
    """
    conn = get_connection(db_type='extracting')
    
    if not conn:
        return []
    
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("SELECT * FROM extracted_jobs ORDER BY scraped_at DESC")
        rows = cursor.fetchall()
        
        return [dict(row) for row in rows]
        
    except Exception as e:
        print(f"Error retrieving all extracted jobs: {e}")
        return []
        
    finally:
        close_connection(conn)


def get_cleaned_job_count():
    """
    Get total count of cleaned jobs (complete + incomplete).
    
    Returns:
        int: Total count, or 0 if error
    """
    conn = get_connection(db_type='cleaning')
    
    if not conn:
        return 0
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM cleaned_jobs")
        complete = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM cleaned_jobs_incomplete")
        incomplete = cursor.fetchone()[0]
        
        return complete + incomplete
    
    except Exception as e:
        print(f"Error counting cleaned jobs: {e}")
        return 0
        
    finally:
        close_connection(conn)


def cleaned_job_already_exists(posting_unique_id):
    """
    Check if a job has already been cleaned (in either table).
    
    Args:
        posting_unique_id (str): Unique job ID
    
    Returns:
        bool: True if exists, False otherwise
    """
    conn = get_connection(db_type='cleaning')
    
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        # Check complete table
        cursor.execute("SELECT COUNT(*) FROM cleaned_jobs WHERE posting_unique_id = ?", (posting_unique_id,))
        if cursor.fetchone()[0] > 0:
            return True
        
        # Check incomplete table
        cursor.execute("SELECT COUNT(*) FROM cleaned_jobs_incomplete WHERE posting_unique_id = ?", (posting_unique_id,))
        if cursor.fetchone()[0] > 0:
            return True
        
        return False
    
    except Exception as e:
        print(f"Error checking if job cleaned: {e}")
        return False
        
    finally:
        close_connection(conn)