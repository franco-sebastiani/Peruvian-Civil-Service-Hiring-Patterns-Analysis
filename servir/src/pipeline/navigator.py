"""
Navigation module for paginated job listings.

Handles all page movement and job discovery on SERVIR portal.
"""

import re
from servir.src.config import NAVIGATION_TIMEOUT, PAGE_LOAD_WAIT


async def get_total_pages(page):
    """
    Extract total page count from SERVIR's page indicator.
    
    Args:
        page: Playwright page object
    
    Returns:
        int: Total number of pages, or 0 if unable to determine
    """
    try:
        page_indicator = await page.locator('text=/Página \\d+ de \\d+/').first.text_content(timeout=5000)
        match = re.search(r'Página (\d+) de (\d+)', page_indicator)
        if match:
            return int(match.group(2))
    except Exception as e:
        print(f"Could not determine total pages: {e}")
    
    return 0


async def get_jobs_on_current_page(page):
    """
    Get list of job indices available on the current page.
    
    Each page has multiple "Ver más" buttons, one per job.
    This returns a list of indices [0, 1, 2, ...] to iterate through.
    
    Args:
        page: Playwright page object (on listing page)
    
    Returns:
        list: Job indices (0-based) on this page
    """
    try:
        buttons = await page.locator('button:has-text("Ver más")').all()
        return list(range(len(buttons)))
    except Exception as e:
        print(f"Error getting jobs on page: {e}")
        return []


async def navigate_next_page(page):
    """
    Click the "Next" pagination button to advance to the next page.
    
    Checks if the button is disabled before clicking.
    
    Args:
        page: Playwright page object
    
    Returns:
        bool: True if navigation succeeded, False otherwise
    """
    try:
        next_btn = page.locator('button.btn-paginator:has-text("Sig.")').first
        
        # Check if button is disabled
        is_disabled = await next_btn.evaluate('el => el.getAttribute("aria-disabled")')
        
        if is_disabled == "true":
            return False
        
        # Click next button
        await next_btn.click()
        await page.wait_for_timeout(PAGE_LOAD_WAIT)
        
        return True
        
    except Exception as e:
        print(f"Error navigating to next page: {e}")
        return False