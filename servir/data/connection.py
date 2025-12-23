"""
Database connection management for SERVIR data.
"""

import sqlite3
from pathlib import Path


def get_db_path(db_type='collection'):
    """
    Get the path to a SERVIR database file.
    
    Args:
        db_type: 'collection' (raw data) or 'processed' (cleaned data)
    
    Returns:
        Path: Path object pointing to the database file
    
    Raises:
        ValueError: If db_type is invalid
        OSError: If unable to create directory structure
    """
    if db_type == 'collection':
        db_path = Path(__file__).parent.parent.parent / "data" / "raw" / "servir_jobs.db"
    elif db_type == 'processed':
        db_path = Path(__file__).parent.parent.parent / "data" / "processed" / "servir_jobs_processed.db"
    else:
        raise ValueError(f"Invalid db_type: {db_type}. Must be 'collection' or 'processed'")
    
    try:
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return db_path
    except OSError as e:
        print(f"Error creating database directory: {e}")
        raise


def get_connection(db_type='collection'):
    """
    Create and return a database connection.
    
    Args:
        db_type: 'collection' (raw data) or 'processed' (cleaned data)
    
    Returns:
        sqlite3.Connection: Active database connection, or None if failed
    """
    try:
        db_path = get_db_path(db_type)
        conn = sqlite3.connect(db_path)
        return conn
    
    except sqlite3.Error as e:
        print(f"Database connection error: {e}")
        return None
    
    except Exception as e:
        print(f"Unexpected error connecting to database: {e}")
        return None


def close_connection(conn):
    """
    Safely close a database connection.
    
    Args:
        conn (sqlite3.Connection): Connection object to close
    """
    if conn:
        try:
            conn.close()
        except sqlite3.Error as e:
            print(f"Error closing database connection: {e}")