"""
Validation module for job posting data.

Checks if extracted job data is complete and valid.
"""

from servir.src.collecting.config.config import REQUIRED_FIELDS


def is_data_complete(job_data):
    """
    Check if all required fields are present and non-None.
    
    Args:
        job_data: dict with job offer data
    
    Returns:
        bool: True if all fields are present and non-None, False otherwise
    """
    if not job_data:
        return False
    
    return all(job_data.get(field) is not None for field in REQUIRED_FIELDS)


def get_missing_fields(job_data):
    """
    Get list of field names that are None in job_data.
    
    Args:
        job_data: dict with job offer data
    
    Returns:
        list: Field names that have None values
    """
    if not job_data:
        return REQUIRED_FIELDS.copy()
    
    return [field for field in REQUIRED_FIELDS if job_data.get(field) is None]


def validate_job_data(job_data):
    """
    Return validation result with details.
    
    Args:
        job_data: dict with job offer data
    
    Returns:
        dict: {
            'is_valid': bool,
            'missing_fields': list,
            'errors': list
        }
    """
    result = {
        'is_valid': is_data_complete(job_data),
        'missing_fields': get_missing_fields(job_data),
        'errors': []
    }
    
    if not job_data:
        result['errors'].append("job_data is None or empty")
    
    return result