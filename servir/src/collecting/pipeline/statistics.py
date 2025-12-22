"""
Statistics tracking for data collection pipeline.

Tracks metrics throughout execution without affecting the pipeline logic.
Can be easily swapped or extended.
"""

from datetime import datetime


class PipelineStats:
    """
    Track statistics throughout collection run.
    
    Separates metrics tracking from execution logic.
    """
    
    def __init__(self):
        self.start_time = datetime.now()
        self.end_time = None
        
        # Page and job counts
        self.pages_processed = 0
        self.jobs_encountered = 0
        
        # Job outcomes
        self.jobs_saved_complete = 0
        self.jobs_saved_incomplete = 0
        self.jobs_skipped_duplicate = 0
        self.jobs_failed = 0
        
        # Duplicate tracking
        self.consecutive_duplicates = 0
        
        # Error log
        self.errors = []
    
    def record_job_encountered(self):
        """Record that a job was encountered."""
        self.jobs_encountered += 1
    
    def record_job_saved_complete(self):
        """Record that a complete job was saved."""
        self.jobs_saved_complete += 1
        self.consecutive_duplicates = 0  # Reset on successful save
    
    def record_job_saved_incomplete(self):
        """Record that an incomplete job was saved."""
        self.jobs_saved_incomplete += 1
        self.consecutive_duplicates = 0  # Reset on successful save
    
    def record_duplicate(self):
        """Record that a duplicate was skipped."""
        self.jobs_skipped_duplicate += 1
        self.consecutive_duplicates += 1
    
    def record_failed(self):
        """Record that a job failed."""
        self.jobs_failed += 1
    
    def record_error(self, error_msg):
        """Add an error message to the error log."""
        self.errors.append(error_msg)
    
    def record_page_processed(self):
        """Record that a page was processed."""
        self.pages_processed += 1
    
    def finish(self):
        """Mark collection as finished."""
        self.end_time = datetime.now()
    
    def get_duration_seconds(self):
        """Get duration in seconds."""
        end = self.end_time or datetime.now()
        return (end - self.start_time).total_seconds()
    
    def get_duration_minutes(self):
        """Get duration in minutes."""
        return self.get_duration_seconds() / 60
    
    def print_summary(self, initial_count=0, final_count=0):
        """
        Print a clean summary report.
        
        Args:
            initial_count: Job count before collection
            final_count: Job count after collection
        """
        total_jobs = (self.jobs_saved_complete + self.jobs_saved_incomplete + 
                      self.jobs_skipped_duplicate + self.jobs_failed)
        
        print("\n" + "="*70)
        print("COLLECTION SUMMARY")
        print("="*70)
        print(f"Duration: {self.get_duration_seconds():.1f}s ({self.get_duration_minutes():.1f}m)")
        print(f"Pages processed: {self.pages_processed}")
        print(f"Jobs encountered: {self.jobs_encountered}")
        print(f"\nJob outcomes:")
        print(f"  Saved (complete): {self.jobs_saved_complete}")
        print(f"  Saved (incomplete): {self.jobs_saved_incomplete}")
        print(f"  Skipped (duplicate): {self.jobs_skipped_duplicate}")
        print(f"  Failed: {self.jobs_failed}")
        
        if initial_count > 0 or final_count > 0:
            new_jobs = final_count - initial_count
            print(f"\nDatabase:")
            print(f"  Before: {initial_count} jobs")
            print(f"  After: {final_count} jobs")
            print(f"  New: {new_jobs}")
        
        if self.errors:
            print(f"\nErrors ({len(self.errors)}):")
            for error in self.errors[:10]:
                print(f"  - {error}")
            if len(self.errors) > 10:
                print(f"  ... and {len(self.errors) - 10} more")
        else:
            print(f"\nNo errors!")
        
        print("="*70 + "\n")