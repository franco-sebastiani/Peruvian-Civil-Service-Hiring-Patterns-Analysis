# Data Collection Plan: Peru Public Sector Job Allocations

## Overview
This document outlines the strategy for collecting data on public sector job allocations in Peru to investigate the extent to which pre-arranged agreements, rather than merit criteria, determine hiring outcomes.

---

## Strategy 1: SERVIR Job Postings (Current Focus)
**Status: In Development**

### Data Source
- **Portal:** Talento Perú (https://app.servir.gob.pe/DifusionOfertasExterno/)
- **Data Type:** Active and recently closed job postings
- **Collection Method:** Web scraping with daily updates
- **Retention Logic:** All postings with closing dates on t-1 or later remain visible

### Data Volume (as of 6 Dec 2024)
- **Total Pages:** 347 pages
- **Postings per Page:** 5 job postings
- **Total Estimated Postings:** ~1,735 postings
- **Date Range:** Latest posting started 24 Nov 2024, ending 5 Dec 2024

### Data Fields to Collect
| Field Name | Spanish Name | Raw Data Type | Notes |
|---|---|---|---|
| posting_id | NÚMERO DE CONVOCATORIA | String (numeric) | Unique identifier, e.g., "735144" |
| institution | INSTITUCIÓN | String | Full institution name |
| job_title | Aviso title | String | Job position title |
| number_of_vacancies | CANTIDAD DE VACANTES | Integer | Number of positions available |
| experience_requirements | EXPERIENCIA | String (unstructured) | Free text, may vary in format |
| academic_profile | FORMACIÓN ACADÉMICA - PERFIL | String (unstructured) | Education requirements, often vague |
| specialization | ESPECIALIZACIÓN | String (unstructured) | Specialization needed, optional |
| knowledge | CONOCIMIENTO | String (unstructured) | Technical knowledge, optional |
| competencies | COMPETENCIAS | String (unstructured) | Required competencies, optional |
| salary | REMUNERACIÓN | String (numeric with currency) | Format: "S/. 4,000.00" (needs parsing) |
| posting_start_date | FECHA INICIO DE PUBLICACIÓN | String (date) | Format: DD/MM/YYYY, e.g., "05/12/2025" |
| posting_end_date | FECHA FIN DE PUBLICACIÓN | String (date) | Format: DD/MM/YYYY, e.g., "22/12/2025" |
| convocatoria_number | NÚMERO DE CONVOCATORIA (full) | String | Format: "D.LEG 1057 - DETERMINADO..." (contains contract type) |
| scrape_date | (system generated) | DateTime | Timestamp of scrape, format: YYYY-MM-DD HH:MM:SS |

### Data Processing Pipeline (To Be Built)
**Phase 1: Raw Data Collection**
- Scrape fields as-is from SERVIR portal
- Store in SQLite with raw string/text types

**Phase 2: Data Cleaning & Standardization**
- Parse `salary` → extract numeric value and standardize currency (all PEN)
- Parse `posting_start_date` and `posting_end_date` → convert to ISO format (YYYY-MM-DD)
- Parse `convocatoria_number` → extract contract type (CAS, PLAZA PRESUPUESTADA, etc.)
- Standardize institution names (handle variations, abbreviations)
- Handle missing/vague values (e.g., "DE ACUERDO A LAS BASES ADMINISTRATIVAS")

**Phase 3: Data Quality Flagging**
- Flag records with missing or vague requirements
- Flag salary inconsistencies
- Flag data quality issues for manual review

**Phase 4: Analysis Dataset**
- Export cleaned data to `data/processed/servir_postings_cleaned.csv`
- Ready for analysis

### Collection Strategy
- **Frequency:** Weekly manual scraping (every Monday, or fixed day)
- **Method:** Run `python src/scrape_servir.py` manually once per week
- **Scope:** Capture all currently visible postings with timestamp
- **Storage:** SQLite database (`data/raw/servir_postings.db`)
  - Table: `postings` with columns (posting_id PRIMARY KEY, institution, job_title, experience, academic_profile, contract_type, salary, posting_date, closing_date, scrape_date)
- **Deduplication:** Database primary key on posting_id prevents duplicates; only new postings are inserted
- **Tracking:** `scrape_date` column records when each posting was first collected
- **Export:** Convert to CSV for analysis phase (`data/processed/servir_postings_analyzed.csv`)
- **Test Period:** 3 months (13 weekly scrapes for initial validation)
- **Full Period:** 2+ years (if test successful)

### Website Structure
- **Pagination:** Results paginated (347 pages × 5 postings per page)
- **Detail Page:** Each posting links to a new page with full details
- **Click Interaction:** Click "Ver más" to load detail page for each posting
- **Unique Identifier:** Each posting has a unique ID; institution name also serves as identifier

### Data Quality Rules
**Include postings that have:**
- All required fields (institution, job title, requirements, contract type, salary, dates)

**Exclude postings that have:**
- Missing or vague requirements (e.g., "visit our website" instead of actual requirements)
- Incomplete salary information
- Institutions that don't follow standard posting format

**Rationale:** Heterogeneous data from institutions that don't provide complete information will introduce noise without meaningful analytical value.

### Known Challenges & Decisions
1. **Reposted Jobs:** Unclear if same job can be reposted. Decision needed: treat each posting as independent record OR implement deduplication logic.
2. **Dynamic Content:** Portal uses JavaScript rendering; requires headless browser for scraping (Selenium or Playwright).
3. **Rate Limiting:** Unknown if SERVIR has rate-limiting policies; must scrape responsibly.
4. **Data Gaps:** Some institutions won't provide required fields; these records will be filtered out.
5. **Language:** All content in Spanish; may require preprocessing for standardization.

### Data Processing Pipeline (To Be Built)
1. Scrape SERVIR portal daily
2. Parse HTML/JavaScript-rendered pages
3. Extract posting details from new detail pages
4. Validate against data quality rules
5. Store in structured format (CSV/database)
6. Log any missing or problematic records

---

## Strategy 2: Ministry of Education Hiring Results (Future)
**Status: Not Started - Defer to Phase 2**

### Data Source
- **Portal:** Ministry of Education hiring results PDFs
- **Period:** 2016-2025
- **Data Type:** Candidate scores and final hiring decisions

### Expected Data Fields
- Candidate Names
- CV Score
- Academic Test Score
- Interview Score
- Final Score
- Winner Candidate

### Notes for Future Implementation
- Requires PDF extraction and parsing
- Format consistency across years needs validation
- May lack job description details for contextualizing merit criteria
- Would enable comparison between advertised requirements (Strategy 1) and actual hiring outcomes (Strategy 2)

---

## Next Steps
1. ✅ Explore SERVIR website structure manually
2. Build proof-of-concept scraper for 1 page (5 postings)
3. Test detail page extraction
4. Implement pagination and full-day collection
5. Run 1-week test period
6. Expand to 3-month test period
7. Document data quality issues and filtering decisions
8. Commit cleaned data to GitHub