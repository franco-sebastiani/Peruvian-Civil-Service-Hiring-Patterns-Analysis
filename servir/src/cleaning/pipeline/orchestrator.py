"""
Cleaning pipeline orchestrator.

Main coordinator that reads raw jobs, cleans them, and writes to cleaned database.
"""

from datetime import datetime
from servir.src.cleaning.database.schema import initialize_database
from servir.src.cleaning.database.operations import insert_cleaned_job, insert_cleaned_job_incomplete
from servir.src.cleaning.database.queries import get_cleaned_job_count, cleaned_job_already_exists, get_all_extracted_jobs
from servir.src.cleaning.pipeline.job_cleaner import clean_job
from servir.src.cleaning.pipeline.statistics import ProcessingStats


def clean_all_jobs():
    """
    Main cleaning pipeline orchestrator.
    
    Coordinates:
    1. Database initialization
    2. Read all raw jobs from extracting database
    3. For each job: clean → decide → save to cleaned database
    4. Report statistics
    
    Returns:
        ProcessingStats: Statistics from the run
    """
    
    print("\n" + "="*70)
    print("CLEANING PIPELINE - DATA CLEANING")
    print("="*70)
    print(f"Started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
    
    # Initialize database
    print("-"*70)
    print("Initializing cleaned database...")
    print("-"*70)
    
    if not initialize_database():
        print("Failed to initialize cleaned database. Stopping.")
        return None
    
    initial_count = get_cleaned_job_count()
    print(f"Database ready. Current count: {initial_count} cleaned jobs\n")
    
    # Initialize statistics
    stats = ProcessingStats()
    
    # Read jobs
    print("-"*70)
    print("Reading jobs from extracting database...")
    print("-"*70)
    
    raw_jobs = get_all_extracted_jobs()
    
    if not raw_jobs:
        print("No jobs found in extracting database.")
        return None
    
    print(f"Found {len(raw_jobs)} jobs to clean\n")
    print("-"*70)
    print("CLEANING JOBS")
    print("-"*70 + "\n")
    
    # Process each job
    for idx, raw_job in enumerate(raw_jobs, 1):
        posting_id = raw_job.get('posting_unique_id')
        stats.record_job_encountered()
        
        # Check if already cleaned
        if cleaned_job_already_exists(posting_id):
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
            success, msg = insert_cleaned_job(cleaned_job)
            if success:
                stats.record_job_saved_complete()
                outcome = "[OK]"
            else:
                stats.record_failed()
                stats.record_error(msg)
                outcome = "[FAILED]"
        else:
            # Incomplete job
            success, msg = insert_cleaned_job_incomplete(cleaned_job, failed_fields)
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
    print("CLEANING COMPLETE")
    print("="*70)
    
    stats.finish()
    final_count = get_cleaned_job_count()
    
    print(f"\nCompleted at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    stats.print_summary(initial_count, final_count)
    
    return stats


if __name__ == "__main__":
    clean_all_jobs()