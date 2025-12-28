"""
Main orchestrator for SERVIR job extraction pipeline.

Coordinates all pipeline modules (navigator, job_processor, statistics).
Contains the core extraction loop.
"""

from datetime import datetime
from playwright.async_api import async_playwright

from servir.src.extracting.config.config import SERVIR_URL, CONSECUTIVE_DUPLICATES_THRESHOLD
from servir.src.extracting.pipeline.navigator import get_total_pages, get_jobs_on_current_page, navigate_next_page
from servir.src.extracting.pipeline.job_processor import extract_job_with_retry, decide_job_action
from servir.src.extracting.pipeline.statistics import PipelineStats
from servir.src.extracting.database.schema import initialize_database
from servir.src.extracting.database.operations import insert_extracted_job, insert_extracted_job_incomplete
from servir.src.extracting.database.queries import get_extracted_job_count, extracted_job_exists


async def collect_all_servir_jobs():
    """
    Main data extraction orchestrator.
    
    Coordinates:
    1. Database setup
    2. Browser initialization
    3. Page-by-page iteration
    4. Job extraction and validation
    5. Database storage
    6. Final reporting
    
    Returns:
        PipelineStats: Statistics from the extraction run
    """
    
    print("\n" + "="*70)
    print("SERVIR JOB EXTRACTION PIPELINE")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Initialize database
    print("-"*70)
    print("Initializing database...")
    print("-"*70)
    
    if not initialize_database():
        print("Failed to initialize database. Stopping.")
        return None
    
    initial_count = get_extracted_job_count()
    print(f"Database ready. Current count: {initial_count} jobs\n")
    
    # Initialize statistics
    stats = PipelineStats()
    
    # Launch browser and start extraction
    async with async_playwright() as p:
        browser = await p.firefox.launch()
        page = await browser.new_page()
        
        try:
            # Load SERVIR portal
            print("-"*70)
            print("Loading SERVIR portal...")
            print("-"*70)
            
            try:
                response = await page.goto(SERVIR_URL, wait_until="networkidle", timeout=30000)
                
                # Check HTTP status
                if response and response.status != 200:
                    print(f"Portal returned HTTP {response.status}. Stopping.")
                    await browser.close()
                    return None
                
                await page.wait_for_timeout(3000)
                
                # Verify we got the real SERVIR page, not an error page
                title = await page.title()
                if "error" in title.lower() or "cloudflare" in title.lower():
                    print(f"Portal loaded error page: '{title}'")
                    print("SERVIR may be down or experiencing issues. Try again later.")
                    await browser.close()
                    return None
                
                # Verify we can find the pagination
                total_pages = await get_total_pages(page)
                if total_pages == 0:
                    print("Portal loaded but could not find pagination.")
                    print("SERVIR page structure may have changed.")
                    await browser.close()
                    return None
                
                # Only now can we confidently say it loaded
                print("Portal loaded successfully")
                print(f"Total pages: {total_pages}\n")
                
            except Exception as e:
                print(f"Failed to load portal: {e}")
                await browser.close()
                return None
            
            print("-"*70)
            print("EXTRACTING JOBS")
            print("-"*70 + "\n")
            
            # Main loop: page by page
            current_page = 1
            
            while current_page <= total_pages:
                
                # Re-check total pages (SERVIR updates dynamically)
                updated_total = await get_total_pages(page)
                if updated_total > 0 and updated_total != total_pages:
                    print(f"Page count updated: {total_pages} → {updated_total}\n")
                    total_pages = updated_total
                
                print(f"Page {current_page}/{total_pages}")
                
                # Get jobs on this page
                job_indices = await get_jobs_on_current_page(page)
                print(f"  {len(job_indices)} jobs found")
                
                # Process each job on this page
                for job_idx in job_indices:
                    job_number = job_idx + 1  # Convert 0-indexed to 1-indexed for display
                    stats.record_job_encountered()
                    
                    # Extract job with retry
                    job_data, is_valid, missing = await extract_job_with_retry(page, job_idx)
                    
                    # Decide what to do with the job
                    decision = decide_job_action(job_data, is_valid, current_page, job_idx)
                    
                    # Determine outcome
                    outcome = ""
                    
                    # Execute decision
                    if decision['action'] == 'failed':
                        stats.record_failed()
                        stats.record_error(decision['message'])
                        posting_id = decision['posting_id']
                        id_str = f" ({posting_id})" if posting_id else ""
                        outcome = f"[FAILED] {decision['message'].split(': ', 1)[-1]}{id_str}"
                    
                    elif decision['action'] == 'ready_to_save_complete':
                        posting_id = decision['posting_id']
                        
                        # Check for duplicate
                        if extracted_job_exists(posting_id):
                            stats.record_duplicate()
                            outcome = "[DUPLICATE]"
                            
                            # Consecutive duplicates detection
                            if stats.consecutive_duplicates >= CONSECUTIVE_DUPLICATES_THRESHOLD:
                                print(f"    Job {job_number}: {outcome}")
                                print(f"\n  Reached previous extraction point ({CONSECUTIVE_DUPLICATES_THRESHOLD} duplicates).")
                                print(f"  Stopping to avoid wasting resources.\n")
                                
                                stats.finish()
                                await browser.close()
                                
                                final_count = get_extracted_job_count()
                                stats.print_summary(initial_count, final_count)
                                
                                return stats
                        else:
                            # Save to database
                            success, msg = insert_extracted_job(decision['data'])
                            if success:
                                stats.record_job_saved_complete()
                                outcome = "[OK]"
                            else:
                                stats.record_failed()
                                stats.record_error(msg)
                                outcome = f"[FAILED] {msg}"
                    
                    elif decision['action'] == 'ready_to_save_incomplete':
                        # Save incomplete job for manual review
                        success, msg = insert_extracted_job_incomplete(
                            decision['data'], 
                            decision['missing_fields']
                        )
                        if success:
                            stats.record_job_saved_incomplete()
                            outcome = "[INCOMPLETE]"
                        else:
                            stats.record_failed()
                            stats.record_error(msg)
                            outcome = f"[FAILED] {msg}"
                    
                    print(f"    Job {job_number}: {outcome}")
                
                stats.record_page_processed()
                
                # Move to next page if not last
                if current_page < total_pages:
                    if not await navigate_next_page(page):
                        print(f"Failed to navigate to page {current_page + 1}. Stopping.\n")
                        break
                
                current_page += 1
        
        except Exception as e:
            print(f"\n✗ Critical error: {e}")
            import traceback
            traceback.print_exc()
        
        finally:
            await browser.close()
    
    # Finish and report
    stats.finish()
    final_count = get_extracted_job_count()
    
    print("-"*70)
    print("EXTRACTION COMPLETE")
    print("-"*70)
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    stats.print_summary(initial_count, final_count)
    
    return stats


if __name__ == "__main__":
    import asyncio
    asyncio.run(collect_all_servir_jobs())