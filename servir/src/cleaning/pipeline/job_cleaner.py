"""
Job cleaner for processing pipeline.

Applies all parsers to a single raw job and returns cleaned data.
"""

from servir.src.cleaning.parsers.salary_parser import transform_salary
from servir.src.cleaning.parsers.vacancy_parser import transform_vacancy
from servir.src.cleaning.parsers.date_parser import transform_date
from servir.src.cleaning.parsers.contract_parser import transform_contract_type
from servir.src.cleaning.parsers.text_parser import clean_text
from servir.src.cleaning.parsers.job_title_parser import clean_job_title
from servir.src.cleaning.parsers.knowledge_parser import clean_knowledge
from servir.src.cleaning.parsers.competencies_parser import clean_competencies
from servir.src.cleaning.parsers.experience_parser import clean_experience
from servir.src.cleaning.parsers.academic_parser import clean_academic_profile
from servir.src.cleaning.parsers.specialization_parser import clean_specialization


def clean_job(raw_job):
    """
    Clean a single raw job by applying all parsers.
    
    Processing steps:
    1. Parse salary → standardized amount
    2. Parse vacancy → integer count
    3. Parse dates → ISO format
    4. Parse contract type → standardized category
    5. Clean text fields → trimmed, punctuation removed
    6. Remove Roman numerals from job title (e.g., "ASISTENTE II" → "ASISTENTE")
    
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
        
        # Clean text fields using field-specific parsers
        # Job title: remove markers, gender, numerals, then generic cleaning
        title_result = clean_job_title(raw_job.get('job_title'))
        
        # Institution: generic cleaning only (no job-title-specific logic)
        institution_result = clean_text(raw_job.get('institution'))
        
        # Other fields: use their specific parsers
        experience_result = clean_experience(raw_job.get('experience_requirements'))
        academic_result = clean_academic_profile(raw_job.get('academic_profile'))
        specialization_result = clean_specialization(raw_job.get('specialization'))
        knowledge_result = clean_knowledge(raw_job.get('knowledge'))
        competencies_result = clean_competencies(raw_job.get('competencies'))
        
        # Build cleaned job
        cleaned_job = {
            'posting_unique_id': posting_id,
            'job_title': title_result,
            'institution': institution_result,
            'posting_start_date': start_date_result['date_iso'],
            'posting_end_date': end_date_result['date_iso'],
            'salary_amount': salary_result['salary_amount'],
            'number_of_vacancies': vacancy_result['vacancy_count'],
            'contract_type': contract_result['contract_type'],
            'experience_requirements': experience_result,
            'academic_profile': academic_result,
            'specialization': specialization_result,
            'knowledge': knowledge_result,
            'competencies': competencies_result
        }
        
        # Identify failed fields (None values)
        failed_fields = [field for field, value in cleaned_job.items() if value is None]
        
        return cleaned_job, failed_fields
    
    except Exception as e:
        return None, ['all_fields']