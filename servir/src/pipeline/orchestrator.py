"""
Main orchestrator for SERVIR job collection pipeline.

Coordinates all pipeline modules (navigator, job_processor, statistics).
Contains the core collection loop.
"""

from datetime import datetime
from playwright.async_api import async_playwright

from servir.src.config.config import SERVIR_URL, CONSECUTIVE_DUPLICATES_THRESHOLD
from servir.src.pipeline.navigator import get_total_pages, get_jobs_on_current_page, navigate_next_page
from servir.src.pipeline.job_processor import extract_job_with_retry, decide_job_action
from servir.src.pipeline.statistics import PipelineStats
from servir.src.database.schema import initialize_database
from servir.src.database.operations import insert_job_offer, insert_job_offer_incomplete
from servir.src.database.queries import get_job_count, job_exists


async def collect_all_servir_jobs():
    """
    Main data collection orchestrator.
    
    Coordinates:
    1. Database setup
    2. Browser initialization
    3. Page-by-page iteration
    4. Job extraction and validation
    5. Database storage
    6. Final reporting
    
    Returns:
        PipelineStats: Statistics from the collection run
    """
    
    print("\n" + "="*70)
    print("SERVIR JOB COLLECTION PIPELINE")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Initialize database
    print("-"*70)
    print("Initializing database...")
    print("-"*70)
    
    if not initialize_database():
        print("Failed to initialize database. Stopping.")
        return None
    
    initial_count = get_job_count()
    print(f"Database ready. Current count: {initial_count} jobs\n")
    
    # Initialize statistics
    stats = PipelineStats()
    
    # Launch browser and start collection
    async with async_playwright() as p:
        browser = await p.firefox.launch()
        page = await browser.new_page()
        
        try:
            # Load SERVIR portal
            print("-"*70)
            print("Loading SERVIR portal...")
            print("-"*70)
            await page.goto(SERVIR_URL, wait_until="networkidle")
            await page.wait_for_timeout(3000)
            print("Portal loaded\n")
            
            # Get total pages
            total_pages = await get_total_pages(page)
            if total_pages == 0:
                print("Could not determine page count. Stopping.")
                return None
            
            print(f"Total pages: {total_pages}\n")
            print("-"*70)
            print("SCRAPING JOBS")
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
                        outcome = "[FAILED]"
                    
                    elif decision['action'] == 'ready_to_save_complete':
                        posting_id = decision['posting_id']
                        
                        # Check for duplicate
                        if job_exists(posting_id):
                            stats.record_duplicate()
                            outcome = "[DUPLICATE]"
                            
                            # Stop if we hit consecutive duplicates threshold
                            if stats.consecutive_duplicates >= CONSECUTIVE_DUPLICATES_THRESHOLD:
                                print(f"    Job {job_number}: {outcome}")
                                print(f"\n  Reached previous collection point ({CONSECUTIVE_DUPLICATES_THRESHOLD} duplicates).")
                                print(f"  Stopping to avoid wasting resources.\n")
                                
                                stats.finish()
                                await browser.close()
                                
                                final_count = get_job_count()
                                stats.print_summary(initial_count, final_count)
                                
                                return stats
                        else:
                            # Save to database
                            success, msg = insert_job_offer(decision['data'])
                            if success:
                                stats.record_job_saved_complete()
                                outcome = "[OK]"
                            else:
                                stats.record_failed()
                                stats.record_error(msg)
                                outcome = "[FAILED]"
                    
                    elif decision['action'] == 'ready_to_save_incomplete':
                        # Save incomplete job for manual review
                        success, msg = insert_job_offer_incomplete(
                            decision['data'], 
                            decision['missing_fields']
                        )
                        if success:
                            stats.record_job_saved_incomplete()
                            outcome = "[INCOMPLETE]"
                        else:
                            stats.record_failed()
                            stats.record_error(msg)
                            outcome = "[FAILED]"
                    
                    print(f"    Job {job_number}: {outcome}")
                
                stats.record_page_processed()
                print(f"  ✓ Complete\n")
                
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
    final_count = get_job_count()
    
    print("-"*70)
    print("COLLECTION COMPLETE")
    print("-"*70)
    print(f"Completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    stats.print_summary(initial_count, final_count)
    
    return stats


if __name__ == "__main__":
    import asyncio
    asyncio.run(collect_all_servir_jobs())