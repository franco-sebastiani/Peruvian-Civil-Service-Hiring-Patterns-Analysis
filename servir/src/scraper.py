"""
Web scraper for SERVIR job postings.

Handles browser automation, navigation, and orchestrates the extraction
of job offer data from the SERVIR portal.
"""

from playwright.async_api import async_playwright
from servir.src.extractors.job_assembler import assemble_job_offer


async def scrape_job_offer(job_offer_index=0):
    """
    Scrape a single job offer from the SERVIR portal.
    
    This function handles the complete scraping workflow:
    1. Launch browser and navigate to SERVIR job listings
    2. Find and click on the specified job offer
    3. Extract all job data from the detail page
    4. Return the extracted data (for database storage or debugging)
    5. Clean up browser resources
    
    Args:
        job_offer_index (int): Zero-based index of which job offer to scrape
                               (0 = first job, 1 = second job, etc.)
    
    Returns:
        dict or None: Extracted job offer data, or None if extraction failed
    """
    async with async_playwright() as p:
        browser = await p.firefox.launch()
        page = await browser.new_page()
        
        try:
            # Navigate to SERVIR job listings page
            servir_url = "https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/ofertas_laborales.xhtml"
            await page.goto(servir_url, wait_until="networkidle")
            await page.wait_for_timeout(3000)
            print(f"List page loaded")
            
            # Find all "Ver más" buttons (one per job offer)
            ver_mas_buttons = await page.locator('button:has-text("Ver más")').all()
            print(f"Found {len(ver_mas_buttons)} job offers available\n")
            
            # Validate requested index
            if job_offer_index >= len(ver_mas_buttons):
                print(f"Error: Only {len(ver_mas_buttons)} job offers found. Index {job_offer_index} is out of range.")
                await browser.close()
                return None
            
            # Click on the specified job offer
            print(f"Clicking job offer #{job_offer_index + 1}...")
            await ver_mas_buttons[job_offer_index].click()
            await page.wait_for_timeout(3000)
            print(f"Detail page loaded")
            
            # Extract all job data using the assembler
            job_data = await assemble_job_offer(page)
            
            print("Extraction completed successfully!")
            
            return job_data
            
        except Exception as e:
            print(f"\nError during scraping: {e}")
            import traceback
            traceback.print_exc()
            return None
            
        finally:
            await browser.close()