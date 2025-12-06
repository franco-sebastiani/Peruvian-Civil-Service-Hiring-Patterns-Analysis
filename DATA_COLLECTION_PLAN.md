# Data Collection Plan: Peru Public Sector Job Allocations

## Overview
This document outlines the strategy for collecting data on public sector job allocations in Peru to investigate the extent to which pre-arranged agreements, rather than merit criteria, determine hiring outcomes.

## Strategy 1: SERVIR Job Postings (Current Focus)
*Status: In Development*

### Data Source
- **Portal:** Talento Perú (https://app.servir.gob.pe/DifusionOfertasExterno/)
- **Data Type:** Active job postings and posting details
- **Collection Method:** Daily web scraping

### Data Fields to Collect
- Institution
- Job Title
- Experience Requirements
- Academic Profile
- Contract Type
- Salary
- Posting Date
- Closing Date

### Collection Frequency
- Daily scraping at a fixed time
- Collect all postings from t-1 (previous day)
- Test period: 3 months (initial phase)
- Full period: 2+ years (if viable)

### Known Challenges
- Requires clicking "Ver más" to see full details
- Some institutions may not provide all required fields
- Need to identify unique posting identifiers
- Need to determine if reposted jobs are new records or duplicates

### Data Processing
*(To be defined during implementation)*

---

## Strategy 2: Ministry of Education Hiring Results (Future)
*Status: Not Started - Defer to Phase 2*

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

---

## Next Steps
1. Map SERVIR website structure manually
2. Build data scraping script for Strategy 1
3. Test on 1-week sample data
4. Expand to 3-month test period
5. Document any data quality issues