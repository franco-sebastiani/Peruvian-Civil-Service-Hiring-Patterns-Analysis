"""
Data collection pipeline for SERVIR job postings.

Main orchestrator that coordinates:
- Database initialization
- Web scraping across all pages and jobs
- Data storage
- Progress reporting
"""

import asyncio
from datetime import datetime
from servir.src.scraper import scrape_job_offer
from servir.src.database import (
    initialize_database,
    insert_job_offer,
    job_exists,
    get_job_count
)


async def collect_all_servir_jobs():
    """
    Main data collection pipeline.
    
    Orchestrates the complete workflow:
    1. Initialize database
    2. Loop through all job offers
    3. Scrape each job
    4. Save to database (skip duplicates)
    5. Report progress and final statistics
    
    The pipeline loops through each job offer sequentially:
    - Page 1: jobs 0-9 (10 jobs)
    - Page 2: jobs 10-19 (10 jobs)
    - ...
    - Page 264: jobs 2630-2632 (3 jobs)
    Total: ~2,633 jobs
    
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
        'jobs_encountered': 0,
        'jobs_saved': 0,
        'jobs_skipped_duplicate': 0,
        'jobs_failed': 0,
        'errors': []
    }
    
    # Step 3: Scrape all jobs
    print("\n" + "-"*70)
    print("STEP 2: Scraping All Job Offers")
    print("-"*70)
    print(f"\nStarting to scrape all ~2,633 job offers...\n")
    
    # Based on our test: 263 pages × 10 jobs + 1 page × 3 jobs = 2,633 jobs
    # We'll loop through job indices until scraper returns None (can't find job)
    
    job_index = 0
    consecutive_failures = 0
    max_consecutive_failures = 3  # Stop if 3 scrapes fail in a row
    
    while True:
        try:
            # Attempt to scrape job at this index
            job_data = await scrape_job_offer(job_offer_index=job_index)
            
            if not job_data:
                consecutive_failures += 1
                
                if consecutive_failures >= max_consecutive_failures:
                    print(f"\n✓ Reached end of available jobs (index {job_index})")
                    break
                else:
                    print(f"⚠ Job {job_index}: Failed to scrape (attempt {consecutive_failures}/{max_consecutive_failures})")
                    job_index += 1
                    continue
            
            # Reset consecutive failures counter
            consecutive_failures = 0
            stats['jobs_encountered'] += 1
            
            # Extract posting ID
            posting_id = job_data.get('posting_unique_id')
            
            if not posting_id:
                print(f"✗ Job {job_index}: Missing posting_unique_id")
                stats['jobs_failed'] += 1
                stats['errors'].append(f"Job {job_index}: Missing posting_unique_id")
                job_index += 1
                continue
            
            # Check if already exists
            if job_exists(posting_id):
                stats['jobs_skipped_duplicate'] += 1
                if job_index % 100 == 0:
                    print(f"⊘ Job {job_index}: {posting_id} already exists (skipped)")
                job_index += 1
                continue
            
            # Save to database
            success, message = insert_job_offer(job_data)
            
            if success:
                stats['jobs_saved'] += 1
                if job_index % 100 == 0:
                    print(f"✓ Job {job_index}: Saved {message}")
            else:
                stats['jobs_failed'] += 1
                print(f"✗ Job {job_index}: Failed to save - {message}")
                stats['errors'].append(f"Job {job_index}: {message}")
            
            # Progress report every 250 jobs
            if job_index % 250 == 0 and job_index > 0:
                print(f"\n--- Progress: Job {job_index} ---")
                print(f"    Saved: {stats['jobs_saved']}")
                print(f"    Skipped (duplicates): {stats['jobs_skipped_duplicate']}")
                print(f"    Failed: {stats['jobs_failed']}\n")
            
            job_index += 1
        
        except Exception as e:
            error_msg = f"Job {job_index}: {str(e)}"
            print(f"✗ {error_msg}")
            stats['errors'].append(error_msg)
            stats['jobs_failed'] += 1
            consecutive_failures += 1
            
            if consecutive_failures >= max_consecutive_failures:
                print(f"\nStopping due to repeated failures")
                break
            
            job_index += 1
    
    # Step 4: Final report
    print("\n" + "="*70)
    print("COLLECTION COMPLETE - FINAL REPORT")
    print("="*70)
    
    stats['end_time'] = datetime.now()
    duration = (stats['end_time'] - stats['start_time']).total_seconds()
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Duration: {duration:.1f} seconds ({duration/60:.1f} minutes)")
    
    print(f"\nCollection statistics:")
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