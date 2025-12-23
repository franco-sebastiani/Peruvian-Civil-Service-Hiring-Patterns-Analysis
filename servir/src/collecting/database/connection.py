"""
Database connection management for SERVIR data.

Handles database connection setup, path management, and connection lifecycle.
"""

import sqlite3
from pathlib import Path


def get_db_path():
    """
    Get the path to the SERVIR database file.
    
    The database is stored at: servir/data/raw/servir_jobs.db
    Creates the directory structure if it doesn't exist.
    
    Returns:
        Path: Path object pointing to the database file
    
    Raises:
        OSError: If unable to create directory structure
    """
    
    try:
        # Navigate from src/collection/database/ up to servir/ root, then to data/raw/
        # __file__ = servir/src/collection/database/connection.py
        # parent = servir/src/collection/database/
        # parent.parent = servir/src/collection/
        # parent.parent.parent = servir/src/
        # parent.parent.parent.parent = servir/
        db_path = Path(__file__).parent.parent.parent.parent / "data" / "raw" / "servir_jobs.db"
        
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        return db_path
    except OSError as e:
        print(f"Error creating database directory: {e}")
        raise


def get_connection():
    """
    Create and return a database connection.
    
    Opens a connection to the SQLite database. The caller is responsible
    for closing the connection after use.
    
    Returns:
        sqlite3.Connection: Active database connection object, or None if connection failed
    """
    try:
        db_path = get_db_path()
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