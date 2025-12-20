"""
Job offer assembler for SERVIR job postings.

This module assembles complete job offer data by coordinating field extraction
functions and field configuration.
"""

from servir.config.field_definitions import SIMPLE_FIELDS, REQUIREMENT_FIELDS, FIELD_ORDER
from servir.src.extractors.field_extractors import (
    extract_simple_field,
    extract_requirement_field,
    extract_job_title,
    extract_institution,
    extract_posting_unique_id
)


async def assemble_job_offer(page):
    """
    Assemble complete job offer data from a single job posting detail page.
    
    This function orchestrates the extraction process by:
    1. Extracting all simple fields (salary, dates, vacancies, etc.)
    2. Extracting all requirement fields (experience, education, etc.)
    3. Extracting special fields (job title, institution, unique ID)
    
    The function uses field configurations from servir.config.field_definitions
    to know which fields to extract and what labels to look for.
    
    Args:
        page: Playwright page object positioned on a job detail page
    
    Returns:
        dict: Complete job offer data with all extracted fields. Missing fields 
              have None values. Keys match the field names defined in FIELD_ORDER.
    """
    data = {}
    
    # Extract simple fields (sub-titulo pattern)
    for field_name, label_text in SIMPLE_FIELDS.items():
        value = await extract_simple_field(page, label_text)
        data[field_name] = value
    
    # Extract requirement fields (sub-titulo-2 pattern)
    for field_name, label_text in REQUIREMENT_FIELDS.items():
        value = await extract_requirement_field(page, label_text)
        data[field_name] = value
    
    # Extract special fields (custom patterns)
    data['job_title'] = await extract_job_title(page)
    data['institution'] = await extract_institution(page)
    data['posting_unique_id'] = await extract_posting_unique_id(page)
    
    return data