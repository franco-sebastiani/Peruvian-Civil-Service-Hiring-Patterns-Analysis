# Peruvian Civil Service Hiring Patterns Analysis

## Overview
Data collection and standardisation pipeline for investigating hiring patterns in Peru's civil service. This project demonstrates practical data engineering: web scraping, data cleaning, data transformation, and structured data preparation for analysis.

## Research Hypothesis
Job postings in Peru's civil service exhibit statistically significant deviations from average requirements, suggesting non-meritocratic hiring patterns. This project collects, standardises, and analyzes hiring data to test this hypothesis using econometric methods.

## Methodology

**Phase 1 (Complete): Data Collection**
- Web scrape job postings from SERVIR (Peru's civil service portal)
- Status: 4,000+ job postings collected; pipeline runs continuously to gather new data

**Phase 2 (Complete): Data Cleaning**
- Standardise and normalise job requirement descriptions for comparability
- Pipeline handles: contract type normalisation, text standardisation, whitespace/special character removal, NULL value handling
- Status: Cleaned data stored in SQLite; ready for transformation

**Phase 3 (In Progress): Data Transformation**
- Map job titles to ISCO-08 occupational classifications using hybrid semantic + fuzzy matching
- Match institutions to MEF budget database for comprehensive institutional identifiers
- Manual validation via Streamlit interfaces
- Fine-tuning models on validated data for improved automated classification
- Status: Job title and institution matching systems operational; building validated training datasets

**Phase 4 (Not started): Analysis**
- Apply statistical methods to test hypothesis and identify hiring pattern deviations across institutions

## Getting Started

### Installation
```bash
git clone https://github.com/franco-sebastiani/Peruvian-Civil-Service-Hiring-Patterns-Analysis.git
cd Peruvian-Civil-Service-Hiring-Patterns-Analysis
pip install -r requirements.txt
```

### Data Setup

#### MEF Budget Data (Required for Institution Matching)
Due to file size (7GB), the MEF budget data is not included in this repository.

1. **Download the dataset:**
   - Visit: [MEF Datos Abiertos - Consulta Amigable](https://datosabiertos.mef.gob.pe/dataset/comparacion-de-presupuesto-ejecucion-gasto/resource/a9f4cffc-7d0e-4c7c-ab51-1ff115c5beca)
   - Download: "Comparativo de Gastos 2022-2025" (CSV format)

2. **Save to project:**
   ```bash
   mkdir -p servir/data/reference/MEF_budget
   # Save downloaded CSV as:
   # servir/data/reference/MEF_budget/comparativo_gastos_2022_2025.csv
   ```

3. **Convert CSV to database:**
   ```bash
   python servir/data/reference/MEF_budget/load_mef_to_sqlite.py
   ```
   
   This creates `mef_budget.db` with:
   - `budget_data` table: Full budget records
   - `institutions` table: Unique institution catalog with all identifiers
   
   **Note:** Conversion takes 10-30 minutes for the 7GB file.

### Running the Pipeline
```bash
python main.py
```

Select an option:
- `1` — Data extraction (web scraping)
- `2` — Data cleaning
- `3` — Data transformation (job title & institution matching)
- `4` — Analysis

**Note:** Phases 1 and 2 are fully implemented. Phase 3 is operational. Phase 4 is in progress.

## Technical Stack
- **Languages:** Python
- **Web Scraping:** Playwright
- **Database:** SQLite
- **NLP/Matching:** sentence-transformers, thefuzz
- **Interface:** Streamlit (validation interfaces)
- **Architecture:** Clean separation (schema.py, queries.py, operations.py)

## Project Structure
```
Peruvian-Civil-Service-Hiring-Patterns-Analysis/
├── servir/
│   ├── src/
│   │   ├── extracting/      # Web scraping logic
│   │   ├── cleaning/        # Data standardisation pipeline
│   │   ├── transforming/    # Data transformation & matching
│   │   │   └── transformers/
│   │   │       ├── job_title_transformer/     # ISCO-08 matching
│   │   │       └── institution_transformer/   # MEF institution matching
│   │   └── analysis/        # Statistical analysis (Phase 4)
│   ├── data/
│   │   ├── raw/             # servir_jobs_raw.db
│   │   ├── cleaned/         # servir_jobs_cleaned.db
│   │   ├── transformed/     # Validation databases
│   │   │   ├── job_title/   # Job title → ISCO mappings
│   │   │   └── institution/ # Institution → MEF mappings
│   │   └── reference/       # Reference data
│   │       ├── isco_08_peru.db
│   │       └── MEF_budget/
│   │           └── mef_budget.db (created from CSV - not in repo)
│   └── __init__.py
├── notebooks/               # Exploratory analysis & testing
├── results/                 # Output & visualisations
├── main.py
├── requirements.txt
└── README.md
```

## Key Features
- **Modular architecture** separating extraction, cleaning, transformation, and analysis
- **Clean code principles** with schema/queries/operations separation for database operations
- **Dual matching approach** combining semantic (sentence transformers) and fuzzy (string similarity) matching
- **Comprehensive institution mapping** capturing all MEF identifiers (nivel_gobierno, sector, pliego, ejecutora, etc.)
- **Separate validation databases** for each transformation step, enabling reproducibility
- **Incremental processing** - only processes new/unvalidated records on reruns
- **Streamlit validation interfaces** for manual review and correction of automated matches

## Data Sources

### ISCO-08 Peru Classification
- **File:** `data/reference/isco_08_peru.csv`
- **Source:** [INEI - Listado de Ocupaciones](https://cdn.www.gob.pe/uploads/document/file/4841890/Listado%20de%20Ocupaciones.xlsx?v=1689349996)
- **Retrieved:** 04-02-2026
- **Processing:** Loaded into `isco_08_peru.db` via `load_isco_08_to_sqlite.py`
- **Description:** Peruvian ISCO-08 occupational classification (4 hierarchical levels) from Instituto Nacional de Estadística e Informática
- **Structure:**
  - `isco_level_1`: Gran grupos (broadest)
  - `isco_level_2`: Sub grupos principal
  - `isco_level_3`: Sub grupos
  - `isco_level_4`: Grupos primarios (most specific - used for matching)

### MEF Budget Data
- **File:** `comparativo_gastos_2022_2025.csv` (**not included in repo**)
- **Source:** [MEF Datos Abiertos - Consulta Amigable](https://datosabiertos.mef.gob.pe/)
- **Retrieved:** 2025
- **Processing:** Converted to `mef_budget.db` via `load_mef_to_sqlite.py`
- **Description:** Comprehensive budget execution data (2022-2025) for all public institutions in Peru
- **Key Fields:** 
  - Institutional hierarchy: `NIVEL_GOBIERNO`, `SECTOR`, `PLIEGO`, `EJECUTORA`, `SEC_EJEC`
  - Geographic: Department, province, district
  - Budget phases: PIA, PIM, Certificado, Comprometido, Devengado, Girado
- **Note:** API access blocked outside Peru - use downloaded CSV instead

## Transformation Details

### Job Title Matching
- **Input:** Raw job titles from SERVIR postings
- **Output:** ISCO-08 nivel 4 classifications with confidence scores
- **Method:** 
  - Semantic matching using Spanish sentence transformers (`hiiamsid/sentence_similarity_spanish_es`)
  - Fuzzy matching using token-set ratio
  - Combined ranking by best confidence score
- **Validation:** Manual review via Streamlit interface
- **Future:** Fine-tune transformer model on validated mappings

### Institution Matching
- **Input:** Institution names from SERVIR postings
- **Output:** Complete MEF institutional identifiers (ejecutora, sector, pliego, etc.)
- **Method:** Dual semantic + fuzzy matching against MEF institution catalog
- **Output serves as:** Universal lookup table for linking SERVIR ↔ MEF data
- **Use cases:** Budget analysis, sector comparisons, geographic analysis, institutional hierarchy studies

## .gitignore Notes
Large data files excluded from version control:
```
servir/data/reference/MEF_presupuesto/*.csv
servir/data/reference/MEF_presupuesto/mef_budget.db
```