"""
Data collection pipeline for SERVIR job postings with pagination.

Main orchestrator that coordinates:
- Database initialization
- Web scraping across all pages
- Data storage
- Progress reporting

Architecture:
- Pipeline handles: page navigation, job iteration, database saves
- Scraper handles: extracting a single job's details
- Database handles: storage and deduplication
"""

import asyncio
import re
from datetime import datetime
from playwright.async_api import async_playwright

from servir.src.scraper import scrape_single_job
from servir.src.database.schema import initialize_database
from servir.src.database.operations import insert_job_offer, insert_job_offer_incomplete
from servir.src.database.queries import job_exists, get_job_count


SERVIR_URL = "https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/ofertas_laborales.xhtml"


def _is_data_complete(job_data):
    """
    Check if job data has all 13 required fields filled (no None values).
    
    Args:
        job_data: dict with job offer data
    
    Returns:
        bool: True if all fields are present and non-None, False otherwise
    """
    if not job_data:
        return False
    
    # Check that all values are not None
    for key, value in job_data.items():
        if value is None:
            return False
    
    return True


def _get_missing_fields(job_data):
    """
    Get list of field names that are None in job_data.
    
    Args:
        job_data: dict with job offer data
    
    Returns:
        list: Field names that have None values
    """
    if not job_data:
        return []
    
    missing = []
    for key, value in job_data.items():
        if value is None:
            missing.append(key)
    
    return missing


def _is_data_complete(job_data):
    """
    Check if job data has all 13 required fields filled (no None values).
    
    Args:
        job_data: dict with job offer data
    
    Returns:
        bool: True if all fields are present and non-None, False otherwise
    """
    if not job_data:
        return False
    
    # Check that all values are not None
    for key, value in job_data.items():
        if value is None:
            return False
    
    return True


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
        await page.wait_for_timeout(2000)  # Wait for new page to load
        
        return True
        
    except Exception as e:
        print(f"Error navigating to next page: {e}")
        return False


async def collect_all_servir_jobs():
    """
    Main data collection pipeline.
    
    Orchestrates the complete workflow:
    1. Initialize database
    2. Navigate to SERVIR listing
    3. For each page:
       - Get all jobs on that page
       - Extract each job's details
       - Save to database (skip duplicates)
       - Move to next page
    4. Report statistics
    
    Returns:
        dict: Statistics about the collection run
    """
    
    print("\n" + "="*70)
    print("SERVIR JOB COLLECTION PIPELINE")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    # Step 1: Initialize database
    print("\n" + "-"*70)
    print("STEP 1: Database Initialization")
    print("-"*70)
    
    if not initialize_database():
        print("Failed to initialize database. Stopping.")
        return None
    
    initial_count = get_job_count()
    print(f"Database ready. Currently contains: {initial_count} jobs")
    
    # Step 2: Initialize statistics
    stats = {
        'start_time': datetime.now(),
        'pages_processed': 0,
        'jobs_encountered': 0,
        'jobs_saved': 0,
        'jobs_saved_incomplete': 0,
        'jobs_skipped_duplicate': 0,
        'jobs_failed': 0,
        'consecutive_duplicates': 0,
        'errors': []
    }
    
    # Step 3: Scrape all jobs with pagination
    print("\n" + "-"*70)
    print("STEP 2: Scraping All Job Offers (All Pages)")
    print("-"*70)
    print()
    
    async with async_playwright() as p:
        browser = await p.firefox.launch()
        page = await browser.new_page()
        
        try:
            # Navigate to SERVIR listing
            print(f"Navigating to SERVIR: {SERVIR_URL}")
            await page.goto(SERVIR_URL, wait_until="networkidle")
            await page.wait_for_timeout(3000)
            print("SERVIR listing page loaded\n")
            
            # Get total pages
            total_pages = await get_total_pages(page)
            if total_pages == 0:
                print("Could not determine total pages. Stopping.")
                await browser.close()
                return None
            
            print(f"Total pages to process: {total_pages}")
            print("="*70 + "\n")
            
            # Loop through all pages
            current_page_num = 1
            
            while current_page_num <= total_pages:
                # Re-check total pages each iteration (SERVIR updates dynamically)
                updated_total = await get_total_pages(page)
                if updated_total > 0 and updated_total != total_pages:
                    print(f"Total pages changed: {total_pages} → {updated_total}")
                    total_pages = updated_total
                
                print(f"Processing Page {current_page_num}/{total_pages}")
                print("-"*70)
                
                try:
                    # Get jobs on this page
                    job_indices = await get_jobs_on_current_page(page)
                    print(f"  Found {len(job_indices)} jobs on this page")
                    
                    # Extract each job on this page
                    for job_idx in job_indices:
                        try:
                            stats['jobs_encountered'] += 1
                            
                            # Extract job details (with retry if incomplete)
                            job_data = await scrape_single_job(page, job_idx)
                            
                            if not job_data:
                                stats['jobs_failed'] += 1
                                continue
                            
                            # Check if data is complete (all 13 fields filled)
                            if not _is_data_complete(job_data):
                                # Retry once
                                job_data = await scrape_single_job(page, job_idx)
                                
                                # Check again
                                if not _is_data_complete(job_data):
                                    # Still incomplete, save to incomplete table
                                    posting_id = job_data.get('posting_unique_id')
                                    
                                    if posting_id:
                                        missing_fields = _get_missing_fields(job_data)
                                        success, message = insert_job_offer_incomplete(job_data, missing_fields)
                                        
                                        if success:
                                            stats['jobs_saved_incomplete'] += 1
                                            stats['consecutive_duplicates'] = 0  # Reset counter
                                        else:
                                            stats['jobs_failed'] += 1
                                            stats['errors'].append(f"Page {current_page_num}, Job {job_idx}: Failed to save incomplete - {message}")
                                    else:
                                        stats['jobs_failed'] += 1
                                        stats['errors'].append(f"Page {current_page_num}, Job {job_idx}: Missing posting_unique_id")
                                    
                                    continue
                            
                            # Data is complete, proceed with duplicate check and save
                            posting_id = job_data.get('posting_unique_id')
                            
                            if not posting_id:
                                stats['jobs_failed'] += 1
                                stats['errors'].append(f"Page {current_page_num}, Job {job_idx}: Missing posting_unique_id")
                                continue
                            
                            # Check if already exists
                            if job_exists(posting_id):
                                stats['jobs_skipped_duplicate'] += 1
                                stats['consecutive_duplicates'] += 1
                                
                                # Safeguard: 10 duplicates in a row = reached previous collection point
                                if stats['consecutive_duplicates'] >= 10:
                                    print(f"\nFound 10 consecutive duplicates. Reached previous collection point.")
                                    print(f"Stopping collection to avoid wasting resources.")
                                    await browser.close()
                                    
                                    # Jump to final report
                                    stats['end_time'] = datetime.now()
                                    duration = (stats['end_time'] - stats['start_time']).total_seconds()
                                    
                                    print("\n" + "="*70)
                                    print("COLLECTION STOPPED - REACHED PREVIOUS POINT")
                                    print("="*70)
                                    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                                    print(f"Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
                                    print(f"\nScraping Statistics:")
                                    print(f"  Pages processed: {stats['pages_processed']}")
                                    print(f"  Jobs encountered: {stats['jobs_encountered']}")
                                    print(f"  Jobs saved (complete): {stats['jobs_saved']}")
                                    print(f"  Jobs saved (incomplete): {stats['jobs_saved_incomplete']}")
                                    print(f"  Jobs skipped (duplicate): {stats['jobs_skipped_duplicate']}")
                                    print(f"  Jobs failed: {stats['jobs_failed']}")
                                    print("\n" + "="*70 + "\n")
                                    
                                    return stats
                                
                                continue
                            
                            # Save to database
                            success, message = insert_job_offer(job_data)
                            
                            if success:
                                stats['jobs_saved'] += 1
                                stats['consecutive_duplicates'] = 0  # Reset counter on successful save
                            else:
                                stats['jobs_failed'] += 1
                                stats['errors'].append(f"Page {current_page_num}, Job {job_idx}: {message}")
                        
                        except Exception as e:
                            stats['jobs_failed'] += 1
                            stats['errors'].append(f"Page {current_page_num}, Job {job_idx}: {str(e)}")
                            continue
                    
                    stats['pages_processed'] += 1
                    
                    # Progress report
                    print(f"    Page complete")
                    print(f"    Saved: {stats['jobs_saved']} | Incomplete: {stats['jobs_saved_incomplete']} | Skipped: {stats['jobs_skipped_duplicate']} | Failed: {stats['jobs_failed']}")
                    print()
                    
                    # Move to next page if not last
                    if current_page_num < total_pages:
                        success = await navigate_next_page(page)
                        if not success:
                            print(f"  Failed to navigate to page {current_page_num + 1}. Stopping.")
                            break
                    
                    current_page_num += 1
                    
                except Exception as e:
                    print(f"  Error processing page {current_page_num}: {e}")
                    stats['errors'].append(f"Page {current_page_num}: {str(e)}")
                    import traceback
                    traceback.print_exc()
                    break
        
        except Exception as e:
            print(f"\n✗ Critical error during scraping: {e}")
            import traceback
            traceback.print_exc()
            
        finally:
            await browser.close()
    
    # Step 4: Final report
    print("\n" + "="*70)
    print("COLLECTION COMPLETE - FINAL REPORT")
    print("="*70)
    
    stats['end_time'] = datetime.now()
    duration = (stats['end_time'] - stats['start_time']).total_seconds()
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
    
    print(f"\nScraping Statistics:")
    print(f"  Pages processed: {stats['pages_processed']}/{total_pages}")
    print(f"  Jobs encountered: {stats['jobs_encountered']}")
    print(f"  Jobs saved (complete): {stats['jobs_saved']}")
    print(f"  Jobs saved (incomplete): {stats['jobs_saved_incomplete']}")
    print(f"  Jobs skipped (duplicate): {stats['jobs_skipped_duplicate']}")
    print(f"  Jobs failed: {stats['jobs_failed']}")
    
    final_count = get_job_count()
    new_jobs = final_count - initial_count
    
    print(f"\nDatabase Statistics:")
    print(f"  Before collection: {initial_count} jobs")
    print(f"  After collection: {final_count} jobs")
    print(f"  New jobs added: {new_jobs}")
    
    if stats['errors']:
        print(f"\nErrors Encountered ({len(stats['errors'])}):")
        for error in stats['errors'][:10]:  # Show first 10
            print(f"  - {error}")
        if len(stats['errors']) > 10:
            print(f"  ... and {len(stats['errors']) - 10} more errors")
    else:
        print(f"\n  No errors encountered!")
    
    print("\n" + "="*70 + "\n")
    
    return stats


if __name__ == "__main__":
    asyncio.run(collect_all_servir_jobs())