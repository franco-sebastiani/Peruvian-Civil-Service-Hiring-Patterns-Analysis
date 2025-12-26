"""
Database write operations for SERVIR job postings.

Handles inserting, updating, and deleting job posting records.
All functions return (success, message) tuples for clear error handling.
"""

import sqlite3
from datetime import datetime
from servir.src.database.connection import get_connection, close_connection


def insert_job_offer(job_data):
    """
    Insert a new job offer into the database.
    
    Args:
        job_data (dict): Job offer data dictionary with field names as keys.
                        Must include 'posting_unique_id' at minimum.
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if inserted, False if error or duplicate
            - message: Description of what happened
    """
    print("TESTING")
    # Validate input
    if not job_data:
        return False, "No job data provided"
    
    if not job_data.get('posting_unique_id'):
        return False, "Missing required field: posting_unique_id"
    
    conn = get_connection()
    
    if not conn:
        return False, "Failed to connect to database"
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO job_postings (
                posting_unique_id,
                job_title,
                institution,
                monthly_salary,
                number_of_vacancies,
                posting_start_date,
                posting_end_date,
                contract_type_raw,
                experience_requirements,
                academic_profile,
                specialization,
                knowledge,
                competencies,
                scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job_data.get('posting_unique_id'),
            job_data.get('job_title'),
            job_data.get('institution'),
            job_data.get('monthly_salary'),
            job_data.get('number_of_vacancies'),
            job_data.get('posting_start_date'),
            job_data.get('posting_end_date'),
            job_data.get('contract_type_raw'),
            job_data.get('experience_requirements'),
            job_data.get('academic_profile'),
            job_data.get('specialization'),
            job_data.get('knowledge'),
            job_data.get('competencies'),
            datetime.now()
        ))
        
        conn.commit()
        
        # Build success message
        job_id = job_data.get('posting_unique_id')
        job_title = job_data.get('job_title', 'Unknown Title')
        return True, f"{job_id} - {job_title}"
        
    except sqlite3.IntegrityError:
        # Duplicate posting_unique_id
        job_id = job_data.get('posting_unique_id')
        return False, f"Job {job_id} already exists in database"
        
    except Exception as e:
        return False, f"Database error: {str(e)}"
        
    finally:
        close_connection(conn)


def insert_job_offer_incomplete(job_data, missing_fields):
    """
    Insert a job offer with missing fields into the incomplete table.
    
    Used for jobs that failed extraction and need manual review.
    Tracks which fields were missing and provides space for manual fixes.
    
    Args:
        job_data (dict): Job offer data (may have None values)
        missing_fields (list): List of field names that are None
                              e.g., ['monthly_salary', 'number_of_vacancies']
    
    Returns:
        tuple: (success: bool, message: str)
    """
    # Validate input
    if not job_data:
        return False, "No job data provided"
    
    if not job_data.get('posting_unique_id'):
        return False, "Missing required field: posting_unique_id"
    
    conn = get_connection()
    
    if not conn:
        return False, "Failed to connect to database"
    
    try:
        cursor = conn.cursor()
        
        # Convert missing_fields list to comma-separated string
        missing_fields_str = ", ".join(missing_fields) if missing_fields else ""
        
        cursor.execute("""
            INSERT INTO job_postings_incomplete (
                posting_unique_id,
                job_title,
                institution,
                monthly_salary,
                number_of_vacancies,
                posting_start_date,
                posting_end_date,
                contract_type_raw,
                experience_requirements,
                academic_profile,
                specialization,
                knowledge,
                competencies,
                missing_fields,
                reviewed,
                scraped_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job_data.get('posting_unique_id'),
            job_data.get('job_title'),
            job_data.get('institution'),
            job_data.get('monthly_salary'),
            job_data.get('number_of_vacancies'),
            job_data.get('posting_start_date'),
            job_data.get('posting_end_date'),
            job_data.get('contract_type_raw'),
            job_data.get('experience_requirements'),
            job_data.get('academic_profile'),
            job_data.get('specialization'),
            job_data.get('knowledge'),
            job_data.get('competencies'),
            missing_fields_str,
            False,  # reviewed = FALSE initially
            datetime.now()
        ))
        
        conn.commit()
        
        # Build success message
        job_id = job_data.get('posting_unique_id')
        job_title = job_data.get('job_title', 'Unknown Title')
        return True, f"{job_id} - {job_title} (incomplete: {missing_fields_str})"
        
    except sqlite3.IntegrityError:
        # Duplicate posting_unique_id
        job_id = job_data.get('posting_unique_id')
        return False, f"Job {job_id} already exists in incomplete table"
        
    except Exception as e:
        return False, f"Database error: {str(e)}"
        
    finally:
        close_connection(conn)


def update_job_offer(posting_unique_id, updated_fields):
    """
    Update an existing job offer in the database.
    
    Args:
        posting_unique_id (str): Unique ID of the job to update
        updated_fields (dict): Dictionary of fields to update
                              {field_name: new_value}
    
    Returns:
        tuple: (success: bool, message: str)
    """
    if not updated_fields:
        return False, "No fields provided to update"
    
    conn = get_connection()
    
    if not conn:
        return False, "Failed to connect to database"
    
    try:
        cursor = conn.cursor()
        
        # Build UPDATE query dynamically based on provided fields
        set_clause = ", ".join([f"{field} = ?" for field in updated_fields.keys()])
        values = list(updated_fields.values()) + [posting_unique_id]
        
        query = f"UPDATE job_postings SET {set_clause} WHERE posting_unique_id = ?"
        cursor.execute(query, values)
        
        conn.commit()
        
        if cursor.rowcount > 0:
            return True, f"Updated job {posting_unique_id}"
        else:
            return False, f"Job {posting_unique_id} not found in database"
        
    except Exception as e:
        return False, f"Update error: {str(e)}"
        
    finally:
        close_connection(conn)


def delete_job_offer(posting_unique_id):
    """
    Delete a job offer from the database.
    
    Args:
        posting_unique_id (str): Unique ID of the job to delete
    
    Returns:
        tuple: (success: bool, message: str)
    """
    conn = get_connection()
    
    if not conn:
        return False, "Failed to connect to database"
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            DELETE FROM job_postings 
            WHERE posting_unique_id = ?
        """, (posting_unique_id,))
        
        conn.commit()
        
        if cursor.rowcount > 0:
            return True, f"Deleted job {posting_unique_id}"
        else:
            return False, f"Job {posting_unique_id} not found in database"
        
    except Exception as e:
        return False, f"Delete error: {str(e)}"
        
    finally:
        close_connection(conn)