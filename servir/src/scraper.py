"""
Web scraper for SERVIR job postings.

Handles browser automation, navigation, and orchestrates the extraction
of job offer data from the SERVIR portal.
"""

from playwright.async_api import async_playwright
from servir.src.extractors.job_assembler import assemble_job_offer


# scraper.py
async def scrape_single_job(page, job_index):
    """
    Extract ONE job from the detail page.
    Assumes: page is already on listing, job hasn't been clicked yet.
    """
    # Click the job
    buttons = await page.locator('button:has-text("Ver más")').all()
    await buttons[job_index].click()
    await page.wait_for_timeout(2000)
    
    # Extract
    job_data = await assemble_job_offer(page)
    
    # Go back to listing
    await page.go_back()
    await page.wait_for_timeout(1500)
    
    return job_data