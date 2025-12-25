"""
Job cleaner for processing pipeline.

Applies all parsers to a single raw job and returns cleaned data.
"""

from servir.src.processing.parsers.salary_parser import transform_salary
from servir.src.processing.parsers.vacancy_parser import transform_vacancy
from servir.src.processing.parsers.date_parser import transform_date
from servir.src.processing.parsers.contract_parser import transform_contract_type
from servir.src.processing.parsers.text_parser import transform_text


def clean_job(raw_job):
    """
    Clean a single raw job by applying all parsers.
    
    Args:
        raw_job: dict with raw job data from collection database
    
    Returns:
        tuple: (cleaned_job_dict, failed_fields_list)
            - cleaned_job_dict: job with all fields parsed/cleaned
            - failed_fields_list: list of fields that are None after parsing
    """
    
    posting_id = raw_job.get('posting_unique_id')
    
    if not posting_id:
        return None, ['posting_unique_id']
    
    try:
        # Parse salary
        salary_result = transform_salary(raw_job.get('monthly_salary'))
        
        # Parse vacancy
        vacancy_result = transform_vacancy(raw_job.get('number_of_vacancies'))
        
        # Parse dates
        start_date_result = transform_date(raw_job.get('posting_start_date'))
        end_date_result = transform_date(raw_job.get('posting_end_date'))
        
        # Parse contract type
        contract_result = transform_contract_type(raw_job.get('contract_type_raw'))
        
        # Clean text fields
        # Job title: remove Roman numerals (e.g., "ASISTENTE II" → "ASISTENTE")
        title_result = transform_text(raw_job.get('job_title'), remove_roman_numerals=True)
        
        # Institution: clean text but keep numerals (might be part of institution name)
        institution_result = transform_text(raw_job.get('institution'))
        
        # Other text fields: clean text, no Roman numeral removal
        experience_result = transform_text(raw_job.get('experience_requirements'))
        academic_result = transform_text(raw_job.get('academic_profile'))
        specialization_result = transform_text(raw_job.get('specialization'))
        knowledge_result = transform_text(raw_job.get('knowledge'))
        competencies_result = transform_text(raw_job.get('competencies'))
        
        # Build cleaned job
        cleaned_job = {
            'posting_unique_id': posting_id,
            'job_title': title_result['text'],
            'institution': institution_result['text'],
            'posting_start_date': start_date_result['date_iso'],
            'posting_end_date': end_date_result['date_iso'],
            'salary_amount': salary_result['salary_amount'],
            'number_of_vacancies': vacancy_result['vacancy_count'],
            'contract_type': contract_result['contract_type'],
            'experience_requirements': experience_result['text'],
            'academic_profile': academic_result['text'],
            'specialization': specialization_result['text'],
            'knowledge': knowledge_result['text'],
            'competencies': competencies_result['text']
        }
        
        # Identify failed fields (None values)
        failed_fields = [field for field, value in cleaned_job.items() if value is None]
        
        return cleaned_job, failed_fields
    
    except Exception as e:
        return None, ['all_fields']