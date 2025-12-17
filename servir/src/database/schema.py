"""
Database schema management for SERVIR job postings.

Defines table structures and handles schema initialization.
This module is responsible for creating and maintaining the database structure.
"""

from servir.src.database.connection import get_connection, close_connection


def create_job_postings_table():
    """
    Create the job_postings table if it doesn't exist.
    
    Table Structure:
    ---------------
    - id: INTEGER PRIMARY KEY (auto-incrementing)
    - posting_unique_id: TEXT UNIQUE NOT NULL (SERVIR job ID, e.g., "736308")
    - job_title: TEXT (job position title)
    - institution: TEXT (government institution name)
    - monthly_salary: TEXT (salary information)
    - number_of_vacancies: TEXT (number of open positions)
    - posting_start_date: TEXT (publication date)
    - posting_end_date: TEXT (application deadline)
    - contract_type_raw: TEXT (type of contract)
    - experience_requirements: TEXT (required experience)
    - academic_profile: TEXT (educational requirements)
    - specialization: TEXT (required specialization)
    - knowledge: TEXT (required knowledge areas)
    - competencies: TEXT (required competencies)
    - scraped_at: TIMESTAMP NOT NULL (when data was collected)
    
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


def initialize_database():
    """
    Initialize the complete database schema.
    
    Creates all necessary tables for the SERVIR data collection project.
    This function should be called once at the start of data collection
    to ensure the database is ready.
    
    Currently creates:
    - job_postings table
    
    Future tables can be added here as the project expands.
    
    Returns:
        bool: True if initialization successful, False otherwise
    
    Example:
        # At the start of your pipeline
        if initialize_database():
            print("Database ready for data collection")
        else:
            print("Failed to initialize database")
    """
    print("Initializing database...")
    
    success = create_job_postings_table()
    
    if success:
        print("Database initialized successfully")
        return True
    else:
        print("Database initialization failed")
        return False