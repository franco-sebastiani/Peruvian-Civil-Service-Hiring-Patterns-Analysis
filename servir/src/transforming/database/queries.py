"""
Database query operations for transformed jobs.

Handles essential read operations for the transforming phase:
- Checking if jobs exist (duplicate detection)
- Retrieving individual records
- Counting total jobs (progress tracking)
"""

import sqlite3
from servir.src.database.connection import get_connection, close_connection


def job_exists(posting_unique_id):
    """
    Check if a transformed job already exists in the database.
    
    Used for duplicate detection before inserting.
    
    Args:
        posting_unique_id (str): Unique ID to check
    
    Returns:
        bool: True if job exists, False otherwise
    """
    conn = get_connection(db_type='transforming')
    
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) FROM transformed_jobs 
            WHERE posting_unique_id = ?
        """, (posting_unique_id,))
        
        count = cursor.fetchone()[0]
        return count > 0
        
    except Exception as e:
        print(f"Error checking if job exists: {e}")
        return False
        
    finally:
        close_connection(conn)


def get_job_count():
    """
    Get the total number of transformed jobs in the database.
    
    Used for progress tracking and statistics during transformation.
    
    Returns:
        int: Total count of transformed jobs, or 0 if error
    """
    conn = get_connection(db_type='transforming')
    
    if not conn:
        return 0
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM transformed_jobs")
        count = cursor.fetchone()[0]
        
        return count
        
    except Exception as e:
        print(f"Error counting jobs: {e}")
        return 0
        
    finally:
        close_connection(conn)


def get_job_by_id(posting_unique_id):
    """
    Retrieve a single transformed job by its unique ID.
    
    Used for verification after transformation.
    
    Args:
        posting_unique_id (str): Unique ID of the job
    
    Returns:
        dict or None: Job data as dictionary with column names as keys,
                     or None if not found
    """
    conn = get_connection(db_type='transforming')
    
    if not conn:
        return None
    
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM transformed_jobs 
            WHERE posting_unique_id = ?
        """, (posting_unique_id,))
        
        row = cursor.fetchone()
        
        return dict(row) if row else None
        
    except Exception as e:
        print(f"Error retrieving job: {e}")
        return None
        
    finally:
        close_connection(conn)