"""
Database schema for job title validation.

Defines table structures for storing ISCO-08 matching results.
"""

CREATE_JOB_TITLE_MATCHES_TABLE = """
CREATE TABLE IF NOT EXISTS job_title_matches (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    job_title TEXT NOT NULL,
    candidate_codigo TEXT NOT NULL,
    candidate_descripcion TEXT NOT NULL,
    semantic_confidence INTEGER,
    fuzzy_confidence INTEGER,
    best_confidence INTEGER,
    rank INTEGER NOT NULL,
    validated INTEGER DEFAULT 0,
    validated_codigo TEXT,
    validated_descripcion TEXT,
    notes TEXT,
    UNIQUE(job_title, candidate_codigo)
)
"""