"""
Web scraper for extracting a single SERVIR job offer.

Handles extracting one job's details from the detail page,
then returning to the listing page.
"""

from servir.src.extractors.job_assembler import assemble_job_offer


async def scrape_single_job(page, job_index):
    """
    Extract ONE job from the SERVIR listing page.
    
    Process:
    1. Get fresh button references on listing page
    2. Click the specified job
    3. Extract all details from detail page
    4. Return to listing page
    
    Args:
        page: Playwright page object (should be on listing page)
        job_index: Zero-based index of job to extract (0 = first job)
    
    Returns:
        dict: Job data with all fields, or None if extraction failed
    """
    try:
        # Get FRESH button references each time
        # This is critical - buttons change after page navigation
        buttons = await page.locator('button:has-text("Ver más")').all()
        
        # Validate index
        if job_index >= len(buttons):
            print(f"    ✗ Job {job_index}: Index out of range (only {len(buttons)} jobs on page)")
            return None
        
        # Click the job to open detail page
        print(f"    Clicking job {job_index}...", end=" ", flush=True)
        await buttons[job_index].click()
        await page.wait_for_timeout(2000)
        print("clicked, extracting...", end=" ", flush=True)
        
        # Extract all fields from detail page
        job_data = await assemble_job_offer(page)
        
        if not job_data:
            print("✗ (no data)")
            return None
        
        # Return to listing page
        await page.go_back()
        await page.wait_for_timeout(1500)
        print("✓")
        
        return job_data
        
    except Exception as e:
        print(f"    ✗ Job {job_index}: {str(e)}")
        
        # Try to recover by going back to listing page
        try:
            await page.go_back()
            await page.wait_for_timeout(1500)
        except:
            pass
        
        return None