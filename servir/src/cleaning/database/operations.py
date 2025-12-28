"""
Database write operations for cleaned job postings.

Handles inserting cleaned and standardized jobs into cleaning database.
"""

import sqlite3
from datetime import datetime
from servir.src.database.connection import get_connection, close_connection


def insert_cleaned_job(job_data):
    """
    Insert a cleaned job into the cleaning database.
    
    Args:
        job_data (dict): Cleaned job data with all fields processed
    
    Returns:
        tuple: (success: bool, message: str)
    """
    if not job_data:
        return False, "No job data provided"
    
    if not job_data.get('posting_unique_id'):
        return False, "Missing required field: posting_unique_id"
    
    conn = get_connection(db_type='cleaning')
    
    if not conn:
        return False, "Failed to connect to cleaning database"
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO cleaned_jobs (
                posting_unique_id,
                job_title,
                institution,
                posting_start_date,
                posting_end_date,
                salary_amount,
                number_of_vacancies,
                contract_type,
                experience_requirements,
                academic_profile,
                specialization,
                knowledge,
                competencies,
                cleaned_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job_data.get('posting_unique_id'),
            job_data.get('job_title'),
            job_data.get('institution'),
            job_data.get('posting_start_date'),
            job_data.get('posting_end_date'),
            job_data.get('salary_amount'),
            job_data.get('number_of_vacancies'),
            job_data.get('contract_type'),
            job_data.get('experience_requirements'),
            job_data.get('academic_profile'),
            job_data.get('specialization'),
            job_data.get('knowledge'),
            job_data.get('competencies'),
            datetime.now()
        ))
        
        conn.commit()
        
        posting_id = job_data.get('posting_unique_id')
        return True, f"Saved: {posting_id}"
    
    except sqlite3.IntegrityError:
        posting_id = job_data.get('posting_unique_id')
        return False, f"Job {posting_id} already exists in cleaning database"
    
    except Exception as e:
        return False, f"Database error: {str(e)}"
    
    finally:
        close_connection(conn)


def insert_cleaned_job_incomplete(job_data, failed_fields):
    """
    Insert a job with cleaning failures into the incomplete table.
    
    Args:
        job_data (dict): Partially cleaned job data
        failed_fields (list): List of field names that failed cleaning
    
    Returns:
        tuple: (success: bool, message: str)
    """
    if not job_data:
        return False, "No job data provided"
    
    if not job_data.get('posting_unique_id'):
        return False, "Missing required field: posting_unique_id"
    
    conn = get_connection(db_type='cleaning')
    
    if not conn:
        return False, "Failed to connect to cleaning database"
    
    try:
        cursor = conn.cursor()
        
        failed_fields_str = ", ".join(failed_fields) if failed_fields else ""
        
        cursor.execute("""
            INSERT INTO cleaned_jobs_incomplete (
                posting_unique_id,
                job_title,
                institution,
                posting_start_date,
                posting_end_date,
                salary_amount,
                number_of_vacancies,
                contract_type,
                experience_requirements,
                academic_profile,
                specialization,
                knowledge,
                competencies,
                failed_fields,
                reviewed,
                cleaned_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job_data.get('posting_unique_id'),
            job_data.get('job_title'),
            job_data.get('institution'),
            job_data.get('posting_start_date'),
            job_data.get('posting_end_date'),
            job_data.get('salary_amount'),
            job_data.get('number_of_vacancies'),
            job_data.get('contract_type'),
            job_data.get('experience_requirements'),
            job_data.get('academic_profile'),
            job_data.get('specialization'),
            job_data.get('knowledge'),
            job_data.get('competencies'),
            failed_fields_str,
            0,
            datetime.now()
        ))
        
        conn.commit()
        
        posting_id = job_data.get('posting_unique_id')
        return True, f"Saved incomplete: {posting_id} (missing: {failed_fields_str})"
    
    except sqlite3.IntegrityError:
        posting_id = job_data.get('posting_unique_id')
        return False, f"Job {posting_id} already exists in incomplete table"
    
    except Exception as e:
        return False, f"Database error: {str(e)}"
    
    finally:
        close_connection(conn)