"""
Database query operations for processed job postings.

Handles all read operations from the processed database.
"""

import sqlite3


def get_processed_job_count():
    """
    Get total count of processed jobs (complete + incomplete).
    
    Returns:
        int: Total count, or 0 if error
    """
    try:
        from servir.src.database.connection import get_connection, close_connection
        
        conn = get_connection(db_type='processed')
        
        if not conn:
            return 0
        
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM processed_jobs")
        complete = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM processed_jobs_incomplete")
        incomplete = cursor.fetchone()[0]
        
        close_connection(conn)
        
        return complete + incomplete
    
    except Exception as e:
        print(f"Error counting processed jobs: {e}")
        return 0


def job_already_processed(posting_unique_id):
    """
    Check if a job has already been processed (in either table).
    
    Args:
        posting_unique_id (str): Unique job ID
    
    Returns:
        bool: True if exists, False otherwise
    """
    try:
        from servir.src.database.connection import get_connection, close_connection
        
        conn = get_connection(db_type='processed')
        
        if not conn:
            return False
        
        cursor = conn.cursor()
        
        # Check complete table
        cursor.execute("SELECT COUNT(*) FROM processed_jobs WHERE posting_unique_id = ?", (posting_unique_id,))
        if cursor.fetchone()[0] > 0:
            close_connection(conn)
            return True
        
        # Check incomplete table
        cursor.execute("SELECT COUNT(*) FROM processed_jobs_incomplete WHERE posting_unique_id = ?", (posting_unique_id,))
        if cursor.fetchone()[0] > 0:
            close_connection(conn)
            return True
        
        close_connection(conn)
        return False
    
    except Exception as e:
        print(f"Error checking if job processed: {e}")
        return False