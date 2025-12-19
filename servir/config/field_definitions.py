"""
SERVIR Portal Field Mapping

Maps database column names to the label text found on the SERVIR job posting detail pages.
Fields are categorized by extraction method for efficient processing.
Used by the scraper to extract job posting data.
"""

# Simple fields: extracted using sub-titulo labels
SIMPLE_FIELDS = {
    "number_of_vacancies": "CANTIDAD DE VACANTES",
    "monthly_salary": "REMUNERACIÓN",
    "posting_start_date": "FECHA INICIO",
    "posting_end_date": "FECHA FIN DE PUBLICACIÓN",
    "contract_type_raw": "NÚMERO DE CONVOCATORIA",
}

# Requirement fields: extracted from sub-titulo-2 labels in <li> tags
REQUIREMENT_FIELDS = {
    "experience_requirements": "EXPERIENCIA",
    "academic_profile": "FORMACIÓN",
    "specialization": "ESPECIALIZACIÓN",
    "knowledge": "CONOCIMIENTO",
    "competencies": "COMPETENCIAS",
}

# Special fields: extracted using custom logic (CSS selectors, JavaScript, etc.)
SPECIAL_FIELDS = {
    "job_title": "span.sp-aviso0",  # CSS selector
    "institution": "span.sp-aviso",  # CSS selector
    "posting_unique_id": "N°",  # Extracted from sub-titulo-2 text
}

# Combined mapping for reference
FIELD_MAPPING = {
    **SIMPLE_FIELDS,
    **REQUIREMENT_FIELDS,
    **SPECIAL_FIELDS,
}

# Output field order (for displaying and saving to database)
# This determines the order fields appear in extracted data and database rows
FIELD_ORDER = [
    "posting_unique_id",
    "institution",
    "job_title",
    "posting_start_date",
    "posting_end_date",
    "monthly_salary",
    "number_of_vacancies",
    "contract_type_raw",
    "experience_requirements",
    "academic_profile",
    "specialization",
    "knowledge",
    "competencies",
]