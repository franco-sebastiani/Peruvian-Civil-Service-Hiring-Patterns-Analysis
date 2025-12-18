"""
Data collection pipeline for SERVIR job postings.

Main orchestrator that coordinates:
- Browser initialization and management
- Database initialization
- Page navigation
- Web scraping across all pages and jobs
- Data storage
- Progress reporting
"""

import asyncio
from datetime import datetime
from playwright.async_api import async_playwright
from servir.src.scraper import scrape_job_offer
from servir.src.database.schema import initialize_database
from servir.src.database.operations import insert_job_offer
from servir.src.database.queries import job_exists, get_job_count


async def collect_all_servir_jobs():
    """
    Main data collection pipeline.
    
    Orchestrates the complete workflow:
    1. Initialize database
    2. Open browser and navigate to SERVIR
    3. Loop through all pages (1-264)
    4. For each page:
       - Loop through jobs (0-9, or fewer on last page)
       - Scrape each job
       - Save to database
       - Navigate to next page
    5. Close browser and report statistics
    
    Total: ~2,633 jobs across 264 pages
    
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
        print("✗ Failed to initialize database. Stopping.")
        return None
    
    initial_count = get_job_count()
    print(f"✓ Database ready. Currently contains: {initial_count} jobs")
    
    # Step 2: Collection statistics
    stats = {
        'start_time': datetime.now(),
        'pages_processed': 0,
        'jobs_encountered': 0,
        'jobs_saved': 0,
        'jobs_skipped_duplicate': 0,
        'jobs_failed': 0,
        'errors': []
    }
    
    # Step 3: Open browser and scrape all pages
    print("\n" + "-"*70)
    print("STEP 2: Scraping All Job Offers")
    print("-"*70)
    print(f"\nStarting to scrape all ~2,633 job offers...\n")
    
    async with async_playwright() as p:
        browser = await p.firefox.launch()
        page = await browser.new_page()
        
        try:
            # Navigate to SERVIR job listings page
            servir_url = "https://app.servir.gob.pe/DifusionOfertasExterno/faces/consultas/ofertas_laborales.xhtml"
            await page.goto(servir_url, wait_until="networkidle")
            await page.wait_for_timeout(3000)
            print("✓ SERVIR page loaded\n")
            
            total_pages = 264
            total_jobs_scraped = 0
            
            # Loop through all pages
            for page_number in range(1, total_pages + 1):
                try:
                    page_jobs_count = 0
                    
                    # Loop through jobs on this page
                    job_index = 0
                    while True:
                        try:
                            # Scrape job at this index on current page
                            job_data = await scrape_job_offer(page, job_offer_index=job_index)
                            
                            if not job_data:
                                # No more jobs on this page
                                break
                            
                            page_jobs_count += 1
                            total_jobs_scraped += 1
                            stats['jobs_encountered'] += 1
                            
                            # Extract posting ID
                            posting_id = job_data.get('posting_unique_id')
                            
                            if not posting_id:
                                print(f"✗ Page {page_number}, Job {job_index}: Missing posting_unique_id")
                                stats['jobs_failed'] += 1
                                stats['errors'].append(f"Page {page_number}, Job {job_index}: Missing posting_unique_id")
                                job_index += 1
                                continue
                            
                            # Check if already exists
                            if job_exists(posting_id):
                                stats['jobs_skipped_duplicate'] += 1
                                job_index += 1
                                continue
                            
                            # Save to database
                            success, message = insert_job_offer(job_data)
                            
                            if success:
                                stats['jobs_saved'] += 1
                                # Only print first job of page for cleaner output
                                if job_index == 0:
                                    print(f"✓ Page {page_number}: {message}")
                            else:
                                stats['jobs_failed'] += 1
                                print(f"✗ Page {page_number}, Job {job_index}: Failed to save - {message}")
                                stats['errors'].append(f"Page {page_number}, Job {job_index}: {message}")
                            
                            job_index += 1
                        
                        except Exception as e:
                            error_msg = f"Page {page_number}, Job {job_index}: {str(e)}"
                            print(f"✗ {error_msg}")
                            stats['errors'].append(error_msg)
                            stats['jobs_failed'] += 1
                            break  # Stop trying jobs on this page if error
                    
                    # Page completed
                    stats['pages_processed'] += 1
                    print(f"  Page {page_number}: {page_jobs_count} jobs ({total_jobs_scraped} total)")
                    
                    # Progress report every 50 pages
                    if page_number % 50 == 0:
                        print(f"\n--- Progress: Page {page_number}/{total_pages} ---")
                        print(f"    Jobs saved: {stats['jobs_saved']}")
                        print(f"    Jobs skipped: {stats['jobs_skipped_duplicate']}")
                        print(f"    Jobs failed: {stats['jobs_failed']}\n")
                    
                    # Navigate to next page if not on last page
                    if page_number < total_pages:
                        try:
                            # Find and click the "Next" (Sig.) button
                            next_button = page.locator('button.btn-paginator:has-text("Sig.")').first
                            
                            # Check if button exists and is enabled
                            is_disabled = await next_button.evaluate('el => el.getAttribute("aria-disabled")')
                            
                            if is_disabled == "false":
                                await next_button.click()
                                await page.wait_for_timeout(3000)
                            else:
                                print(f"⚠ Next button disabled at page {page_number}")
                                break
                        
                        except Exception as e:
                            error_msg = f"Page {page_number}: Failed to navigate to next page - {str(e)}"
                            print(f"⚠ {error_msg}")
                            stats['errors'].append(error_msg)
                            break
                
                except Exception as e:
                    error_msg = f"Page {page_number}: {str(e)}"
                    print(f"✗ {error_msg}")
                    stats['errors'].append(error_msg)
                    break  # Stop if page processing fails
        
        except Exception as e:
            print(f"✗ Critical error: {e}")
            import traceback
            traceback.print_exc()
            stats['errors'].append(f"Critical error: {e}")
        
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
    
    print(f"\nCollection statistics:")
    print(f"  Pages processed: {stats['pages_processed']}/{total_pages}")
    print(f"  Jobs encountered: {stats['jobs_encountered']}")
    print(f"  Jobs saved: {stats['jobs_saved']}")
    print(f"  Jobs skipped (duplicate): {stats['jobs_skipped_duplicate']}")
    print(f"  Jobs failed: {stats['jobs_failed']}")
    
    final_count = get_job_count()
    new_jobs = final_count - initial_count
    
    print(f"\nDatabase statistics:")
    print(f"  Before collection: {initial_count} jobs")
    print(f"  After collection: {final_count} jobs")
    print(f"  New jobs added: {new_jobs}")
    
    if stats['errors']:
        print(f"\nErrors encountered ({len(stats['errors'])}):")
        for error in stats['errors'][:10]:  # Show first 10
            print(f"  - {error}")
        if len(stats['errors']) > 10:
            print(f"  ... and {len(stats['errors']) - 10} more errors")
    else:
        print(f"\n✓ No errors encountered!")
    
    print("\n" + "="*70 + "\n")
    
    return stats


if __name__ == "__main__":
    # Run the pipeline
    asyncio.run(collect_all_servir_jobs())