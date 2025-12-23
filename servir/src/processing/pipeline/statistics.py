"""
Statistics tracking for processing pipeline.

Tracks metrics throughout processing without affecting pipeline logic.
"""

from datetime import datetime


class ProcessingStats:
    """Track statistics throughout processing run."""
    
    def __init__(self):
        self.start_time = datetime.now()
        self.end_time = None
        
        self.jobs_encountered = 0
        self.jobs_saved_complete = 0
        self.jobs_saved_incomplete = 0
        self.jobs_skipped_duplicate = 0
        self.jobs_failed = 0
        
        self.errors = []
    
    def record_job_encountered(self):
        """Record that a job was encountered."""
        self.jobs_encountered += 1
    
    def record_job_saved_complete(self):
        """Record that a complete job was saved."""
        self.jobs_saved_complete += 1
    
    def record_job_saved_incomplete(self):
        """Record that an incomplete job was saved."""
        self.jobs_saved_incomplete += 1
    
    def record_duplicate(self):
        """Record that a duplicate was skipped."""
        self.jobs_skipped_duplicate += 1
    
    def record_failed(self):
        """Record that a job failed."""
        self.jobs_failed += 1
    
    def record_error(self, error_msg):
        """Add an error message to the error log."""
        self.errors.append(error_msg)
    
    def finish(self):
        """Mark processing as finished."""
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
            initial_count: Job count before processing
            final_count: Job count after processing
        """
        print("\n" + "="*70)
        print("PROCESSING SUMMARY")
        print("="*70)
        print(f"Duration: {self.get_duration_seconds():.1f}s ({self.get_duration_minutes():.1f}m)")
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