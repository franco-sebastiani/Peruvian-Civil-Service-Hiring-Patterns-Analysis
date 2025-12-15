"""
Main entry point for the I_NEED_A_JOB data collection project.

This script orchestrates the data collection pipeline:
- Phase 1: Scrape SERVIR job postings
- Phase 2: Analyze Ministry of Education hiring results (later)
"""

import sys
from pathlib import Path
import asyncio

# Add project root to Python path
sys.path.insert(0, str(Path(__file__).parent))

from src.extractors.extractor_servir import fetch_job_offer


def main():
    """Main entry point for the project."""
    
    print("\n" + "=" * 60)
    print("I_NEED_A_JOB - Data Collection Pipeline")
    print("=" * 60)
    print("\nPhase 1: SERVIR Job Postings Extraction\n")
    
    # For now, test extraction on job offer #1
    # Later this will loop through all 3,470 postings and save to database
    asyncio.run(fetch_job_offer(job_offer_index=0))


if __name__ == "__main__":
    main()