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
- Manually labelling job requirement descriptions using Streamlit interface to create training dataset
- Labelled data stored as JSON; will be used to train categorisation algorithm
- Status: Building training dataset; categorisation algorithm development pending

**Phase 4 (Not started): Analysis**
- Apply statistical methods to test hypothesis and identify hiring pattern deviations across institutions

## Getting Started

### Installation
```bash
git clone https://github.com/franco-sebastiani/Peruvian-Civil-Service-Hiring-Patterns-Analysis.git
cd Peruvian-Civil-Service-Hiring-Patterns-Analysis
pip install -r requirements.txt
```

### Running the Pipeline
```bash
python main.py
```

Select an option:
- `1` — Data extraction (web scraping)
- `2` — Data cleaning
- `3` — Data transformation (labelling interface)
- `4` — Analysis

**Note:** Phases 1 and 2 are fully implemented. Phases 3 and 4 are in progress. Databases are included in the repository.

## Technical Stack
- **Languages:** Python
- **Web Scraping:** Playwright
- **Database:** SQLite
- **Data Format:** JSON (labelled training data)
- **Interface:** Streamlit (labelling app)

## Project Structure
```
Peruvian-Civil-Service-Hiring-Patterns-Analysis/
├── servir/
│   ├── src/
│   │   ├── extracting/      # Web scraping logic
│   │   ├── cleaning/        # Data standardisation pipeline
│   │   ├── transforming/    # Data transformation & categorisation algorithm
│   │   └── analysis/        # Statistical analysis (Phase 4)
│   ├── data/
│   │   ├── raw/             # servir_jobs_raw.db
│   │   ├── cleaned/         # servir_jobs_cleaned.db
│   │   └── transformed/     # [in progress]
│   └── __init__.py
├── notebooks/               # Exploratory analysis & testing
├── results/                 # Output & visualisations
├── main.py
├── requirements.txt
└── README.md
```

## Key Features
- Modular architecture separating data collection, cleaning, transformation, and analysis
- Separate databases for each processing phase, enabling traceability and reproducibility
- Robust data standardisation (contract type normalisation, text standardisation, NULL handling)
- Streamlit-based interface for manual data annotation and training data creation


## Data Sources

### ISCO-08 Peru Classification
- **File:** `data/reference/isco_08_peru.csv`
- **Source:** (https://cdn.www.gob.pe/uploads/document/file/4841890/Listado%20de%20Ocupaciones.xlsx?v=1689349996)
- **Retrieved:** 04-02-2026
- **Processing:** Loaded into `isco_08_peru.db` via `src/load_isco_08_to_sqlite.py`
- **Description:** Peruvian ISCO-08 occupational classification from Instituto Nacional de Estadística e Informática