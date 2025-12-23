"""
Database schema management for SERVIR job postings.

Defines table structures and handles schema initialization.
This module is responsible for creating and maintaining the database structure.
"""

from servir.data.connection import get_connection, close_connection


def create_job_postings_table():
    """
    Create the job_postings table if it doesn't exist.
    
    The posting_unique_id field has a UNIQUE constraint to prevent
    duplicate job postings from being saved.
    
    Returns:
        bool: True if table created/exists, False if error occurred
    """
    conn = get_connection()
    
    if not conn:
        print("Failed to connect to database for table creation")
        return False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_postings (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                posting_unique_id TEXT UNIQUE NOT NULL,
                job_title TEXT,
                institution TEXT,
                monthly_salary TEXT,
                number_of_vacancies TEXT,
                posting_start_date TEXT,
                posting_end_date TEXT,
                contract_type_raw TEXT,
                experience_requirements TEXT,
                academic_profile TEXT,
                specialization TEXT,
                knowledge TEXT,
                competencies TEXT,
                scraped_at TIMESTAMP NOT NULL
            )
        """)
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"Error creating job_postings table: {e}")
        return False
        
    finally:
        close_connection(conn)


def create_job_postings_incomplete_table():
    """
    Create the job_postings_incomplete table if it doesn't exist.
    
    This table stores job postings that failed extraction (missing fields).
    Allows tracking of incomplete data and manual fixes applied.
    
    Returns:
        bool: True if table created/exists, False if error occurred
    """
    conn = get_connection()
    
    if not conn:
        print("Failed to connect to database for table creation")
        return False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS job_postings_incomplete (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                posting_unique_id TEXT UNIQUE NOT NULL,
                job_title TEXT,
                institution TEXT,
                monthly_salary TEXT,
                number_of_vacancies TEXT,
                posting_start_date TEXT,
                posting_end_date TEXT,
                contract_type_raw TEXT,
                experience_requirements TEXT,
                academic_profile TEXT,
                specialization TEXT,
                knowledge TEXT,
                competencies TEXT,
                missing_fields TEXT NOT NULL,
                reviewed BOOLEAN DEFAULT FALSE,
                manually_fixed_fields TEXT,
                notes TEXT,
                scraped_at TIMESTAMP NOT NULL,
                reviewed_at TIMESTAMP
            )
        """)
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"Error creating job_postings_incomplete table: {e}")
        return False
        
    finally:
        close_connection(conn)


def initialize_database():
    """
    Initialize the complete database schema.
    
    Creates all necessary tables for the SERVIR data collection project.
    This function should be called once at the start of data collection
    to ensure the database is ready.
    
    Currently creates:
    - job_postings table (complete jobs)
    - job_postings_incomplete table (jobs missing fields for manual review)
    
    Returns:
        bool: True if initialization successful, False otherwise
    """

    print("Initializing database...")
    
    success1 = create_job_postings_table()
    success2 = create_job_postings_incomplete_table()
    
    if success1 and success2:
        print("Database initialized successfully")
        return True
    else:
        print("Database initialization failed")
        return False