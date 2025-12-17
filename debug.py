"""
Debug utilities for testing and inspecting scraped data.

These functions are for development/testing only. Production code will
save data to a database instead of printing to console.
"""
import asyncio
from servir.src.scraper import scrape_job_offer
from servir.config.field_definitions import FIELD_ORDER


def print_job_data(data):
    """
    Pretty print extracted job offer data in a structured format.
    
    Used for testing and debugging the scraper. In production, data will
    be saved to a database instead.
    
    Args:
        data (dict): Job offer data dictionary with field names as keys
    
    Returns:
        None: Prints directly to console
    """
    print("\n" + "=" * 60)
    print("EXTRACTED JOB POSTING DATA")
    print("=" * 60)
    
    for field_name in FIELD_ORDER:
        value = data.get(field_name)
        
        if value is None:
            display_value = "[NOT FOUND]"
        elif isinstance(value, str) and len(value) > 100:
            display_value = value[:100] + "..."
        else:
            display_value = value
        
        print(f"{field_name:.<35} {display_value}")
    
    print("=" * 60 + "\n")

# In main.py or a test script


async def test():
    job_data = await scrape_job_offer(job_offer_index=0)
    if job_data:
        print_job_data(job_data)

asyncio.run(test())