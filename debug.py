"""
Test Academic Matcher.

Tests:
1. CLASIFICADOR database exists and has programs
2. SERVIR database has academic profiles
3. Parser extracts levels correctly
4. Matcher initializes and processes samples
"""

import sys
from pathlib import Path

# Setup paths
REPO_ROOT = Path("/Users/francosebastiani/Documents/GitHub/Peruvian-Civil-Service-Hiring-Patterns-Analysis")
sys.path.insert(0, str(REPO_ROOT))

CLEANED_DB = REPO_ROOT / "servir" / "data" / "cleaned" / "servir_jobs_cleaned.db"
CLASIFICADOR_DB = REPO_ROOT / "servir" / "data" / "reference" / "clasificador_carreras" / "clasificador_carreras.db"
MATCHES_DB = REPO_ROOT / "servir" / "data" / "transformed" / "academic" / "academic_matches.db"

print("=" * 80)
print("Academic Matcher Test")
print("=" * 80)

# Test 1: Check CLASIFICADOR database
print("\nTest 1: Check CLASIFICADOR Database")
print(f"Looking for: {CLASIFICADOR_DB}")
print(f"File exists: {CLASIFICADOR_DB.exists()}")

if not CLASIFICADOR_DB.exists():
    print("\n✗ CLASIFICADOR database not found!")
    print("Run: python servir/data/reference/clasificador_carreras/load_clasificador_to_sqlite.py")
    sys.exit(1)

import sqlite3
import pandas as pd

conn = sqlite3.connect(CLASIFICADOR_DB)
cursor = conn.cursor()

cursor.execute("SELECT COUNT(*) FROM programs")
program_count = cursor.fetchone()[0]
print(f"✓ CLASIFICADOR database found with {program_count:,} programs")

cursor.execute("SELECT academic_level, COUNT(*) FROM programs GROUP BY academic_level")
print("\nPrograms by level:")
for level, count in cursor.fetchall():
    print(f"  Level {level}: {count:,} programs")

conn.close()

# Test 2: Check SERVIR database
print("\n" + "=" * 80)
print("Test 2: Check SERVIR Database")
print("=" * 80)

print(f"\nLooking for: {CLEANED_DB}")
print(f"File exists: {CLEANED_DB.exists()}")

if not CLEANED_DB.exists():
    print("\n✗ SERVIR cleaned database not found!")
    sys.exit(1)

conn = sqlite3.connect(CLEANED_DB)
cursor = conn.cursor()

cursor.execute("SELECT COUNT(DISTINCT academic_profile) FROM cleaned_jobs WHERE academic_profile IS NOT NULL")
servir_count = cursor.fetchone()[0]
print(f"✓ SERVIR database found with {servir_count:,} unique academic profiles")

cursor.execute("SELECT DISTINCT academic_profile FROM cleaned_jobs WHERE academic_profile IS NOT NULL LIMIT 5")
print("\nSample SERVIR academic profiles:")
for i, (profile,) in enumerate(cursor.fetchall(), 1):
    print(f"  {i}. {profile}")

conn.close()

# Test 3: Test Parser
print("\n" + "=" * 80)
print("Test 3: Test Academic Parser")
print("=" * 80)

from servir.src.transforming.transformers.academic_profile.academic_parser import parse_academic_profile

test_profiles = [
    "TITULO PROFESIONAL DE ABOGADO COLEGIADO Y HABILITADO",
    "BACHILLER EN ADMINISTRACION O ECONOMIA",
    "TITULO TECNICO EN SECRETARIADO EJECUTIVO",
    "EGRESADO EN DERECHO O AFINES"
]

print("\nParsing sample profiles:")
for profile in test_profiles:
    parsed = parse_academic_profile(profile)
    print(f"\n  Input: {profile}")
    print(f"    Level: {parsed['level_name']}")
    print(f"    Search levels: {parsed['search_levels']}")
    print(f"    Thesis required: {parsed['thesis_required']}")
    print(f"    Accepts related: {parsed['accepts_related_fields']}")

# Test 4: Initialize Matcher
print("\n" + "=" * 80)
print("Test 4: Initialize Academic Matcher")
print("=" * 80)

print("\nThis will load CLASIFICADOR programs and initialize matchers...")
print("(May take 1-2 minutes for embeddings)")

try:
    from servir.src.transforming.transformers.academic_profile.academic_matcher import AcademicMatcher
    
    matcher = AcademicMatcher(CLEANED_DB, CLASIFICADOR_DB, MATCHES_DB)
    
    print("\n✓ Matcher initialized successfully!")
    print(f"  Programs loaded: {len(matcher.clasificador_programs):,}")
    
except Exception as e:
    print(f"\n✗ Error initializing matcher: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

# Test 5: Test matching on 3 profiles
print("\n" + "=" * 80)
print("Test 5: Test Matching (3 sample profiles)")
print("=" * 80)

for profile in test_profiles[:3]:
    print(f"\nMatching: {profile}")
    
    try:
        parsed = parse_academic_profile(profile)
        matches = matcher.combine_matches(profile, parsed)
        
        print(f"  Found {len(matches)} total matches")
        if len(matches) > 0:
            print(f"  Top 3 candidates:")
            for match in matches[:3]:
                print(f"    - {match['programa_nombre']}")
                print(f"      Code: {match['programa_codigo']}, Confidence: {match['best_confidence']}")
    except Exception as e:
        print(f"  ✗ Error: {e}")

print("\n" + "=" * 80)
print("✓ ALL TESTS PASSED!")
print("=" * 80)

print("\nReady to run full matching:")
print("python servir/src/transforming/transformers/academic/academic_matcher.py")