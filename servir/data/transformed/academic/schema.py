"""
Database schema for academic profile matching.

Defines table structure for storing CLASIFICADOR matching results.
"""

CREATE_ACADEMIC_MATCHES_TABLE = """
CREATE TABLE IF NOT EXISTS academic_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- From SERVIR
    servir_academic_profile TEXT NOT NULL,
    
    -- Extracted structured fields
    academic_level_code TEXT,
    thesis_required INTEGER,
    accepts_related_fields INTEGER,
    requires_colegiado INTEGER,
    requires_habilitado INTEGER,
    multiple_options_allowed INTEGER,
    
    -- Matched program from CLASIFICADOR
    programa_codigo TEXT NOT NULL,
    programa_nombre TEXT NOT NULL,
    campo_amplio_codigo TEXT,
    campo_amplio_nombre TEXT,
    campo_especifico_codigo TEXT,
    campo_especifico_nombre TEXT,
    campo_detallado_codigo TEXT,
    campo_detallado_nombre TEXT,
    
    -- Matching scores
    semantic_confidence INTEGER,
    fuzzy_confidence INTEGER,
    best_confidence INTEGER,
    rank INTEGER NOT NULL,
    
    -- Manual validation
    validated INTEGER DEFAULT 0,
    validated_programa_codigo TEXT,
    validated_programa_nombre TEXT,
    notes TEXT,
    
    UNIQUE(servir_academic_profile, programa_codigo)
)
"""