"""
Database schema for transformed job postings.

Defines table structure for analysis-ready data with split fields and JSON arrays.
"""

import sqlite3
from servir.src.database.connection import get_connection, close_connection


def initialize_database():
    """
    Initialize the transformed jobs database and create tables.
    
    Creates:
    - transformed_jobs: Analysis-ready jobs with split fields and JSON arrays
    
    Schema details:
    - Dates stored as TEXT in ISO format (YYYY-MM-DD)
    - Arrays stored as JSON: ["item1", "item2", "item3"]
    - Institution kept as TEXT (dimensional modeling in separate phase)
    - Numeric fields maintain proper types (REAL, INTEGER)
    
    Returns:
        bool: True if successful, False otherwise
    """
    conn = get_connection(db_type='transforming')
    
    if not conn:
        print("Failed to connect to transforming database")
        return False
    
    try:
        cursor = conn.cursor()
        
        # Transformed jobs table (analysis-ready)
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS transformed_jobs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                posting_unique_id TEXT UNIQUE NOT NULL,
                job_title TEXT,
                institution TEXT,
                posting_start_date TEXT,
                posting_end_date TEXT,
                salary_amount REAL,
                number_of_vacancies INTEGER,
                contract_type TEXT,
                contract_temporal_nature TEXT,
                experience_general_years INTEGER,
                experience_general_description TEXT,
                experience_general_keywords TEXT,
                experience_specific_years INTEGER,
                experience_specific_description TEXT,
                experience_specific_keywords TEXT,
                academic_level TEXT,
                academic_field TEXT,
                knowledge TEXT,
                competencies TEXT,
                specialization TEXT,
                transformed_at TIMESTAMP NOT NULL
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