"""
Database schema for cleaned job postings.

Defines table structure for cleaned and standardized job data.
"""

import sqlite3
from servir.src.database.connection import get_connection, close_connection


def initialize_database():
    """
    Initialize the cleaned jobs database and create tables.
    
    Creates:
    - cleaned_jobs: Complete, clean jobs ready for transformation
    - cleaned_jobs_incomplete: Jobs with missing/failed fields requiring review
    
    Returns:
        bool: True if successful, False otherwise
    """
    conn = get_connection(db_type='cleaning')
    
    if not conn:
        print("Failed to connect to cleaning database")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Cleaned jobs table (complete, clean data)
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
                cleaned_at TIMESTAMP NOT NULL
            )
        """)
        
        # Cleaned jobs incomplete table (for manual review)
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
                cleaned_at TIMESTAMP NOT NULL
            )
        """)
        
        conn.commit()
        return True
    
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return False
    
    except Exception as e:
        print(f"Unexpected error: {e}")
        return False
    
    finally:
        close_connection(conn)