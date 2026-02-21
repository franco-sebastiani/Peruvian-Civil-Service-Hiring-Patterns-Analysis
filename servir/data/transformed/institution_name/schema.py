"""
Database schema for institution validation.

Defines table structure for storing comprehensive institution matching results
linking SERVIR institutions to MEF budget database identifiers.
"""

CREATE_INSTITUTION_MATCHES_TABLE = """
CREATE TABLE IF NOT EXISTS institution_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    
    -- From SERVIR
    servir_institution_name TEXT NOT NULL,
    
    -- Institutional hierarchy from MEF (all identifier levels)
    nivel_gobierno TEXT,
    nivel_gobierno_nombre TEXT,
    sector TEXT,
    sector_nombre TEXT,
    pliego TEXT,
    pliego_nombre TEXT,
    ejecutora TEXT NOT NULL,
    ejecutora_nombre TEXT NOT NULL,
    sec_ejec TEXT,
    
    -- Geographic information from MEF
    departamento_ejecutora TEXT,
    departamento_ejecutora_nombre TEXT,
    provincia_ejecutora TEXT,
    provincia_ejecutora_nombre TEXT,
    distrito_ejecutora TEXT,
    distrito_ejecutora_nombre TEXT,
    
    -- Matching scores
    semantic_confidence INTEGER,
    fuzzy_confidence INTEGER,
    best_confidence INTEGER,
    rank INTEGER NOT NULL,
    
    -- Manual validation
    validated INTEGER DEFAULT 0,
    validated_ejecutora TEXT,
    validated_ejecutora_nombre TEXT,
    notes TEXT,
    
    UNIQUE(servir_institution_name, ejecutora)
)
"""