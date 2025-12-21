"""
Central configuration for SERVIR data collection pipeline.

Contains all constants, field definitions, and settings.
"""

# ============================================================================
# SERVIR PORTAL CONFIGURATION
# ============================================================================

SERVIR_URL = "https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/ofertas_laborales.xhtml"

# ============================================================================
# FIELD DEFINITIONS
# ============================================================================

# Simple fields: extracted using sub-titulo labels
SIMPLE_FIELDS = {
    "number_of_vacancies": "CANTIDAD DE VACANTES",
    "monthly_salary": "REMUNERACIÓN",
    "posting_start_date": "FECHA INICIO DE PUBLICACIÓN",
    "posting_end_date": "FECHA FIN DE PUBLICACIÓN",
    "contract_type_raw": "NÚMERO DE CONVOCATORIA",
}

# Requirement fields: extracted from sub-titulo-2 labels in <li> tags
REQUIREMENT_FIELDS = {
    "experience_requirements": "EXPERIENCIA",
    "academic_profile": "FORMACIÓN ACADÉMICA",
    "specialization": "ESPECIALIZACIÓN",
    "knowledge": "CONOCIMIENTO",
    "competencies": "COMPETENCIAS",
}

# Special fields: extracted using custom logic (CSS selectors, JavaScript, etc.)
SPECIAL_FIELDS = {
    "job_title": "span.sp-aviso0",  # CSS selector
    "institution": "span.sp-aviso",  # CSS selector
    "posting_unique_id": "Nº",  # Extracted from sub-titulo-2 text
}

# Combined mapping for reference
FIELD_MAPPING = {
    **SIMPLE_FIELDS,
    **REQUIREMENT_FIELDS,
    **SPECIAL_FIELDS,
}

# Output field order (for displaying and saving to database)
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

# All required fields (must be present and non-None for complete data)
REQUIRED_FIELDS = FIELD_ORDER

# ============================================================================
# PIPELINE SETTINGS
# ============================================================================

# Timeouts (in milliseconds)
NAVIGATION_TIMEOUT = 2000
PAGE_LOAD_WAIT = 3000

# Thresholds
CONSECUTIVE_DUPLICATES_THRESHOLD = 10

# Database
DATABASE_PATH = "servir_jobs.db"