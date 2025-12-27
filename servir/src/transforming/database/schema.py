"""
Database schema for transformed jobs.

Defines the structure of processed_jobs_transformed.db table.
This is the final database used for analysis, with extracted and categorized data.
"""

import sqlite3
from servir.src.database.connection import get_connection, close_connection


def create_transformed_jobs_table():
    """
    Create the transformed_jobs table if it doesn't exist.
    
    Schema includes:
    - IDs and metadata (posting_unique_id, institution, job_title)
    - Temporal information (posting dates)
    - Compensation (salary, vacancies)
    - Contract details (type, temporal_nature)
    - Experience requirements (general, specific)
    - Academic requirements (level, field)
    - Skills/competencies (knowledge_items, competencies_items, specialization_items)
    - Audit trail (transformed_at)
    
    Array fields (knowledge_items, etc.) stored as JSON strings.
    
    Returns:
        bool: True if table created/exists, False if error occurred
    """
    conn = get_connection(db_type='transforming')
    
    if not conn:
        print("Failed to connect to database for table creation")
        return False
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transformed_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                posting_unique_id TEXT UNIQUE NOT NULL,
                job_title TEXT,
                institution TEXT,
                posting_start_date TEXT,
                posting_end_date TEXT,
                monthly_salary TEXT,
                number_of_vacancies TEXT,
                contract_type TEXT,
                contract_temporal_nature TEXT,
                experience_general TEXT,
                experience_specific TEXT,
                academic_level TEXT,
                academic_field TEXT,
                knowledge_items TEXT,
                competencies_items TEXT,
                specialization_items TEXT,
                transformed_at TIMESTAMP NOT NULL
            )
        """)
        
        conn.commit()
        return True
        
    except Exception as e:
        print(f"Error creating transformed_jobs table: {e}")
        return False
        
    finally:
        close_connection(conn)


def initialize_database():
    """
    Initialize the complete transformed database schema.
    
    Creates all necessary tables for the transforming phase.
    This function should be called once at the start of transformation
    to ensure the database is ready.
    
    Returns:
        bool: True if initialization successful, False otherwise
    """
    
    print("Initializing transformed database...")
    
    success = create_transformed_jobs_table()
    
    if success:
        print("Transformed database initialized successfully")
        return True
    else:
        print("Transformed database initialization failed")
        return False