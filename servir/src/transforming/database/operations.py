"""
Database write operations for transformed jobs.

Handles inserting and updating transformed job records.
All functions return (success, message) tuples for clear error handling.
"""

import sqlite3
from datetime import datetime
from servir.src.database.connection import get_connection, close_connection


def insert_transformed_job(job_data):
    """
    Insert a new transformed job into the database.
    
    Args:
        job_data (dict): Transformed job data dictionary with field names as keys.
                        Must include 'posting_unique_id' at minimum.
    
    Returns:
        tuple: (success: bool, message: str)
            - success: True if inserted, False if error or duplicate
            - message: Description of what happened
    """
    # Validate input
    if not job_data:
        return False, "No job data provided"
    
    if not job_data.get('posting_unique_id'):
        return False, "Missing required field: posting_unique_id"
    
    conn = get_connection(db_type='transforming')
    
    if not conn:
        return False, "Failed to connect to database"
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            INSERT INTO transformed_jobs (
                posting_unique_id,
                job_title,
                institution,
                posting_start_date,
                posting_end_date,
                salary_amount,
                number_of_vacancies,
                contract_type,
                contract_temporal_nature,
                experience_general,
                experience_specific,
                academic_level,
                academic_field,
                knowledge,
                competencies,
                specialization,
                transformed_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (
            job_data.get('posting_unique_id'),
            job_data.get('job_title'),
            job_data.get('institution'),
            job_data.get('posting_start_date'),
            job_data.get('posting_end_date'),
            job_data.get('salary_amount'),
            job_data.get('number_of_vacancies'),
            job_data.get('contract_type'),
            job_data.get('contract_temporal_nature'),
            job_data.get('experience_general'),
            job_data.get('experience_specific'),
            job_data.get('academic_level'),
            job_data.get('academic_field'),
            job_data.get('knowledge'),
            job_data.get('competencies'),
            job_data.get('specialization'),
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
        return False, f"Job {job_id} already exists in transformed database"
        
    except Exception as e:
        return False, f"Database error: {str(e)}"
        
    finally:
        close_connection(conn)


def update_transformed_job(posting_unique_id, updated_fields):
    """
    Update an existing transformed job in the database.
    
    Args:
        posting_unique_id (str): Unique ID of the job to update
        updated_fields (dict): Dictionary of fields to update
                              {field_name: new_value}
    
    Returns:
        tuple: (success: bool, message: str)
    """
    if not updated_fields:
        return False, "No fields provided to update"
    
    conn = get_connection(db_type='transforming')
    
    if not conn:
        return False, "Failed to connect to database"
    
    try:
        cursor = conn.cursor()
        
        # Build UPDATE query dynamically based on provided fields
        set_clause = ", ".join([f"{field} = ?" for field in updated_fields.keys()])
        values = list(updated_fields.values()) + [posting_unique_id]
        
        query = f"UPDATE transformed_jobs SET {set_clause} WHERE posting_unique_id = ?"
        cursor.execute(query, values)
        
        conn.commit()
        
        if cursor.rowcount > 0:
            return True, f"Updated job {posting_unique_id}"
        else:
            return False, f"Job {posting_unique_id} not found in transformed database"
        
    except Exception as e:
        return False, f"Update error: {str(e)}"
        
    finally:
        close_connection(conn)


def delete_transformed_job(posting_unique_id):
    """
    Delete a transformed job from the database.
    
    Args:
        posting_unique_id (str): Unique ID of the job to delete
    
    Returns:
        tuple: (success: bool, message: str)
    """
    conn = get_connection(db_type='transforming')
    
    if not conn:
        return False, "Failed to connect to database"
    
    try:
        cursor = conn.cursor()
        
        cursor.execute("""
            DELETE FROM transformed_jobs 
            WHERE posting_unique_id = ?
        """, (posting_unique_id,))
        
        conn.commit()
        
        if cursor.rowcount > 0:
            return True, f"Deleted job {posting_unique_id}"
        else:
            return False, f"Job {posting_unique_id} not found in transformed database"
        
    except Exception as e:
        return False, f"Delete error: {str(e)}"
        
    finally:
        close_connection(conn)