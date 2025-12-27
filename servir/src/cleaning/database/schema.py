"""
Database schema for processed job postings.

Defines table structure for cleaned and standardized job data.
"""

import sqlite3
from pathlib import Path


def get_processed_db_path():
    """
    Get the path to the processed jobs database file.
    
    The database is stored at: servir/data/cleaned/servir_jobs_cleaned.db
    Creates the directory structure if it doesn't exist.
    
    Returns:
        Path: Path object pointing to the database file
    """
    try:
        db_path = Path(__file__).parent.parent.parent.parent / "data" / "cleaned" / "servir_jobs_cleaned.db"
        db_path.parent.mkdir(parents=True, exist_ok=True)
        return db_path
    except OSError as e:
        print(f"Error creating database directory: {e}")
        raise


def initialize_database():
    """
    Initialize the processed jobs database and create tables.
    
    Creates:
    - cleaned_jobs: Complete, clean jobs ready for analysis
    - cleaned_jobs_incomplete: Jobs with missing/failed fields requiring review
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        from servir.src.database.connection import get_connection
        
        conn = get_connection(db_type='cleaning')
        
        if not conn:
            print("Failed to connect to processed database")
            return False
        
        cursor = conn.cursor()
        
        # Processed jobs table (complete, clean data)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cleaned_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                posting_unique_id TEXT UNIQUE NOT NULL,
                job_title TEXT,
                institution TEXT,
                posting_start_date TEXT,
                posting_end_date TEXT,
                salary_amount REAL,
                number_of_vacancies INTEGER,
                contract_type TEXT,
                experience_requirements TEXT,
                academic_profile TEXT,
                specialization TEXT,
                knowledge TEXT,
                competencies TEXT,
                processed_at TIMESTAMP NOT NULL
            )
        """)
        
        # Processed jobs incomplete table (for manual review)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS cleaned_jobs_incomplete (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                posting_unique_id TEXT UNIQUE NOT NULL,
                job_title TEXT,
                institution TEXT,
                posting_start_date TEXT,
                posting_end_date TEXT,
                salary_amount REAL,
                number_of_vacancies INTEGER,
                contract_type TEXT,
                experience_requirements TEXT,
                academic_profile TEXT,
                specialization TEXT,
                knowledge TEXT,
                competencies TEXT,
                failed_fields TEXT,
                reviewed INTEGER DEFAULT 0,
                processed_at TIMESTAMP NOT NULL
            )
        """)
        
        conn.commit()
        conn.close()
        
        return True
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return False
    
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False