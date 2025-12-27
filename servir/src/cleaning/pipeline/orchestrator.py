"""
Processing pipeline orchestrator.

Main coordinator that reads raw jobs, cleans them, and writes to processed database.
"""

from datetime import datetime
from servir.src.processing.database.schema import initialize_database
from servir.src.processing.database.operations import insert_processed_job, insert_processed_job_incomplete
from servir.src.processing.database.queries import get_processed_job_count, job_already_processed
from servir.src.collecting.database.queries import get_all_jobs
from servir.src.processing.pipeline.job_cleaner import clean_job
from servir.src.processing.pipeline.statistics import ProcessingStats


def process_all_jobs():
    """
    Main processing pipeline orchestrator.
    
    Coordinates:
    1. Database initialization
    2. Read all raw jobs
    3. For each job: clean → decide → save
    4. Report statistics
    
    Returns:
        ProcessingStats: Statistics from the run
    """
    
    print("\n" + "="*70)
    print("PROCESSING PIPELINE - DATA CLEANING")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Initialize database
    print("-"*70)
    print("Initializing processed database...")
    print("-"*70)
    
    if not initialize_database():
        print("Failed to initialize processed database. Stopping.")
        return None
    
    initial_count = get_processed_job_count()
    print(f"Database ready. Current count: {initial_count} processed jobs\n")
    
    # Initialize statistics
    stats = ProcessingStats()
    
    # Read jobs
    print("-"*70)
    print("Reading jobs from collection database...")
    print("-"*70)
    
    raw_jobs = get_all_jobs()
    
    if not raw_jobs:
        print("No jobs found in collection database.")
        return None
    
    print(f"Found {len(raw_jobs)} jobs to process\n")
    print("-"*70)
    print("PROCESSING JOBS")
    print("-"*70 + "\n")
    
    # Process each job
    for idx, raw_job in enumerate(raw_jobs, 1):
        posting_id = raw_job.get('posting_unique_id')
        stats.record_job_encountered()
        
        # Check if already processed
        if job_already_processed(posting_id):
            stats.record_duplicate()
            if idx % 100 == 0:
                print(f"Job {idx}/{len(raw_jobs)}: [DUPLICATE]")
            continue
        
        # Clean job
        cleaned_job, failed_fields = clean_job(raw_job)
        
        if not cleaned_job:
            stats.record_failed()
            error_msg = f"Job {posting_id}: Failed to clean"
            stats.record_error(error_msg)
            if idx % 100 == 0:
                print(f"Job {idx}/{len(raw_jobs)}: [FAILED]")
            continue
        
        # Save based on completeness
        if not failed_fields:
            # Complete job
            success, msg = insert_processed_job(cleaned_job)
            if success:
                stats.record_job_saved_complete()
                outcome = "[OK]"
            else:
                stats.record_failed()
                stats.record_error(msg)
                outcome = "[FAILED]"
        else:
            # Incomplete job
            success, msg = insert_processed_job_incomplete(cleaned_job, failed_fields)
            if success:
                stats.record_job_saved_incomplete()
                outcome = "[INCOMPLETE]"
            else:
                stats.record_failed()
                stats.record_error(msg)
                outcome = "[FAILED]"
        
        # Progress report
        if idx % 100 == 0:
            print(f"Job {idx}/{len(raw_jobs)}: {outcome}")
    
    # Final report
    print("\n" + "="*70)
    print("PROCESSING COMPLETE")
    print("="*70)
    
    stats.finish()
    final_count = get_processed_job_count()
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    stats.print_summary(initial_count, final_count)
    
    return stats


if __name__ == "__main__":
    process_all_jobs()