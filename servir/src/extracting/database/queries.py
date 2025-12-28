"""
Database query operations for extracted job postings.

Handles all read operations: fetching individual records, filtering,
counting, and analytics. No write operations are performed here.
"""

import sqlite3
from servir.src.database.connection import get_connection, close_connection


def extracted_job_exists(posting_unique_id):
    """
    Check if an extracted job already exists in the database.
    
    Useful for avoiding duplicate extraction or checking before insert.
    
    Args:
        posting_unique_id (str): Unique ID to check
    
    Returns:
        bool: True if job exists, False otherwise
    """
    conn = get_connection(db_type='extracting')
    
    if not conn:
        return False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) FROM extracted_jobs 
            WHERE posting_unique_id = ?
        """, (posting_unique_id,))
        
        count = cursor.fetchone()[0]
        return count > 0
        
    except Exception as e:
        print(f"  Error checking if job exists: {e}")
        return False
        
    finally:
        close_connection(conn)


def get_extracted_job_count():
    """
    Get the total number of extracted job postings in the database.
    
    Returns:
        int: Total count of extracted jobs, or 0 if error
    """
    conn = get_connection(db_type='extracting')
    
    if not conn:
        return 0
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM extracted_jobs")
        count = cursor.fetchone()[0]
        
        return count
        
    except Exception as e:
        print(f"  Error counting jobs: {e}")
        return 0
        
    finally:
        close_connection(conn)


def get_extracted_job_by_id(posting_unique_id):
    """
    Retrieve a single extracted job posting by its unique ID.
    
    Args:
        posting_unique_id (str): Unique ID of the job
    
    Returns:
        dict or None: Job data as dictionary with column names as keys,
                     or None if not found
    """
    conn = get_connection(db_type='extracting')
    
    if not conn:
        return None
    
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM extracted_jobs 
            WHERE posting_unique_id = ?
        """, (posting_unique_id,))
        
        row = cursor.fetchone()
        
        return dict(row) if row else None
        
    except Exception as e:
        print(f"  Error retrieving job: {e}")
        return None
        
    finally:
        close_connection(conn)


def get_all_extracted_jobs(limit=None):
    """
    Retrieve all extracted job postings from the database.
    
    Args:
        limit (int, optional): Maximum number of jobs to return.
                              If None, returns all jobs.
    
    Returns:
        list[dict]: List of job dictionaries, ordered by scrape time (newest first),
                   or empty list if error
    """
    conn = get_connection(db_type='extracting')
    
    if not conn:
        return []
    
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        query = "SELECT * FROM extracted_jobs ORDER BY scraped_at DESC"
        if limit:
            query += f" LIMIT {limit}"
        
        cursor.execute(query)
        rows = cursor.fetchall()
        
        return [dict(row) for row in rows]
        
    except Exception as e:
        print(f"  Error retrieving all jobs: {e}")
        return []
        
    finally:
        close_connection(conn)


def get_extracted_jobs_by_institution(institution_name):
    """
    Get all extracted jobs from a specific institution.
    
    Uses partial matching (LIKE query), so you can search with partial names.
    
    Args:
        institution_name (str): Full or partial institution name
    
    Returns:
        list[dict]: List of matching job dictionaries, or empty list if error
    """
    conn = get_connection(db_type='extracting')
    
    if not conn:
        return []
    
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM extracted_jobs 
            WHERE institution LIKE ?
            ORDER BY scraped_at DESC
        """, (f"%{institution_name}%",))
        
        rows = cursor.fetchall()
        
        return [dict(row) for row in rows]
        
    except Exception as e:
        print(f"  Error retrieving jobs by institution: {e}")
        return []
        
    finally:
        close_connection(conn)


def get_extracted_institution_counts():
    """
    Get count of extracted jobs per institution.
    
    Useful for understanding which institutions have the most jobs extracted.
    
    Returns:
        list[tuple]: List of (institution_name, count) tuples,
                    ordered by count descending (most jobs first),
                    or empty list if error
    """
    conn = get_connection(db_type='extracting')
    
    if not conn:
        return []
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT institution, COUNT(*) as count 
            FROM extracted_jobs 
            GROUP BY institution 
            ORDER BY count DESC
        """)
        
        results = cursor.fetchall()
        
        return results
        
    except Exception as e:
        print(f"  Error getting institution counts: {e}")
        return []
        
    finally:
        close_connection(conn)


def get_recent_extracted_jobs(days=7):
    """
    Get jobs extracted within the last N days.
    
    Args:
        days (int): Number of days to look back (default: 7)
    
    Returns:
        list[dict]: List of recent job dictionaries, or empty list if error
    """
    conn = get_connection(db_type='extracting')
    
    if not conn:
        return []
    
    try:
        conn.row_factory = sqlite3.Row
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT * FROM extracted_jobs 
            WHERE scraped_at >= datetime('now', '-' || ? || ' days')
            ORDER BY scraped_at DESC
        """, (days,))
        
        rows = cursor.fetchall()
        
        return [dict(row) for row in rows]
        
    except Exception as e:
        print(f"  Error retrieving recent jobs: {e}")
        return []
        
    finally:
        close_connection(conn)