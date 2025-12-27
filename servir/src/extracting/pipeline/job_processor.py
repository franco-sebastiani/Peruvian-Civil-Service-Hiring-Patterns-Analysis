"""
Job processing module.

Handles extraction and validation of a single job posting.
Separates extraction logic from database decision logic.
"""

from servir.src.collecting.extractors.scraper import scrape_single_job
from servir.src.collecting.pipeline.job_validator import validate_job_data


async def extract_job_with_retry(page, job_idx, max_retries=1):
    """
    Extract a single job with automatic retry.
    
    If first extraction is incomplete, retry once.
    
    Args:
        page: Playwright page object
        job_idx: Job index on current page
        max_retries: Number of retries if incomplete
    
    Returns:
        tuple: (job_data, is_valid, missing_fields)
            - job_data: dict with extracted data (or None)
            - is_valid: bool, True if all required fields present
            - missing_fields: list of field names that are None
    """
    job_data = None
    
    for attempt in range(max_retries + 1):
        job_data = await scrape_single_job(page, job_idx)
        
        if not job_data:
            continue
        
        validation = validate_job_data(job_data)
        
        if validation['is_valid']:
            return (job_data, True, [])
        
        # If last attempt, return incomplete data
        if attempt == max_retries:
            return (job_data, False, validation['missing_fields'])
    
    return (None, False, [])


def decide_job_action(job_data, is_valid, page_num, job_idx):
    """
    Decide what to do with job data (save complete, save incomplete, or fail).
    
    Separates extraction from database decisions.
    
    Args:
        job_data: extracted job data dict (or None)
        is_valid: bool, True if data is complete
        page_num: current page number (for error reporting)
        job_idx: job index on page (for error reporting)
    
    Returns:
        dict: {
            'action': 'saved_complete' | 'saved_incomplete' | 'failed',
            'posting_id': str or None,
            'data': dict or None,
            'missing_fields': list or None,
            'message': str
        }
    """
    
    posting_id = job_data.get('posting_unique_id') if job_data else None
    
    # No data extracted
    if not job_data:
        return {
            'action': 'failed',
            'posting_id': None,
            'data': None,
            'missing_fields': None,
            'message': f'Page {page_num}, Job {job_idx}: Extraction returned None'
        }
    
    # No posting ID to identify the job
    if not posting_id:
        return {
            'action': 'failed',
            'posting_id': None,
            'data': None,
            'missing_fields': None,
            'message': f'Page {page_num}, Job {job_idx}: Missing posting_unique_id'
        }
    
    # Data is complete
    if is_valid:
        return {
            'action': 'ready_to_save_complete',
            'posting_id': posting_id,
            'data': job_data,
            'missing_fields': None,
            'message': f'Job {posting_id} complete and ready'
        }
    
    # Data is incomplete
    validation = validate_job_data(job_data)
    return {
        'action': 'ready_to_save_incomplete',
        'posting_id': posting_id,
        'data': job_data,
        'missing_fields': validation['missing_fields'],
        'message': f'Job {posting_id} incomplete (missing: {", ".join(validation["missing_fields"])})'
    }