"""
Web scraper for SERVIR job postings.

Handles job extraction from an already-open page.
Browser management and page navigation are handled by the pipeline.
"""

from servir.src.extractors.job_assembler import assemble_job_offer


async def scrape_job_offer(page, job_offer_index=0):
    """
    Scrape a single job offer from the current page.
    
    This function handles extraction only:
    1. Find all "Ver más" buttons on current page
    2. Click on the specified job offer
    3. Extract all job data from the detail page
    4. Go back to the list page
    5. Return the extracted data
    
    Args:
        page: Playwright page object (already open, positioned on SERVIR list page)
        job_offer_index (int): Zero-based index of which job to scrape on this page
                               (0 = first job, 1 = second job, etc.)
                               Valid range: 0-9 (or fewer on last page)
    
    Returns:
        dict or None: Extracted job offer data, or None if extraction failed
    """
    try:
        # Find all "Ver más" buttons on the current page
        ver_mas_buttons = await page.locator('button:has-text("Ver más")').all()
        
        # Validate requested index exists on this page
        if job_offer_index >= len(ver_mas_buttons):
            # No job at this index on current page
            return None
        
        # Click on the specified job offer
        await ver_mas_buttons[job_offer_index].click()
        await page.wait_for_timeout(3000)
        
        # Extract all job data using the assembler
        job_data = await assemble_job_offer(page)
        
        # Go back to the list page
        await page.go_back()
        await page.wait_for_timeout(2000)
        
        return job_data
            
    except Exception as e:
        print(f"Error during scraping (index {job_offer_index}): {e}")
        return None