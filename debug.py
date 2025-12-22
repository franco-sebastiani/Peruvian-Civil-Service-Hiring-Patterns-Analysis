"""
Salary discovery script for data quality audit.

Analyzes raw salaries from collection database to find parsing patterns.
Identifies which salaries can be transformed and which need special handling.
"""

from collections import defaultdict
from servir.src.processing.parsers.salary_parser import transform_salary
from servir.src.collecting.database.queries import get_all_jobs


def discover_salary_patterns():
    """
    Analyze all raw salaries to identify transformation patterns.
    
    Returns:
        dict: Statistics and examples of salary patterns
    """
    
    print("\n" + "="*70)
    print("SALARY DISCOVERY AUDIT")
    print("="*70 + "\n")
    
    # Fetch all jobs from collection database
    print("Reading salaries from collection database...")
    jobs = get_all_jobs()
    
    if not jobs:
        print("No jobs found in collection database.")
        return None
    
    print(f"Found {len(jobs)} jobs\n")
    
    # Categories for results
    successful = []
    parse_failures = defaultdict(list)  # Group by error message
    
    # Process each job's salary
    print("Analyzing salaries (focus on cleaning patterns only)...\n")
    
    for job in jobs:
        posting_id = job.get('posting_unique_id')
        salary_str = job.get('monthly_salary')
        
        # Try to transform
        result = transform_salary(salary_str)
        
        if result['salary_amount'] is not None:
            # Successfully parsed (cleaning worked)
            successful.append({
                'posting_id': posting_id,
                'raw': salary_str,
                'amount': result['salary_amount']
            })
        else:
            # Failed to parse (cleaning didn't work)
            parse_failures[result['error']].append({
                'posting_id': posting_id,
                'raw': salary_str
            })
    
    total = len(jobs)
    print("="*70)
    print("RESULTS")
    print("="*70)
    
    print(f"\nTotal salaries analyzed: {total}")
    print(f"Successfully cleaned: {len(successful)} ({100*len(successful)/total:.1f}%)")
    print(f"Parse failures (cleaning issues): {sum(len(v) for v in parse_failures.values())}")
    
    # Parse failures detail
    if parse_failures:
        print("\n" + "-"*70)
        print("PARSE FAILURES (Cleaning didn't work)")
        print("-"*70)
        for error, examples in parse_failures.items():
            print(f"\n{error} ({len(examples)} cases)")
            for ex in examples[:5]:
                print(f"  ID: {ex['posting_id']}, Raw: '{ex['raw']}'")
            if len(examples) > 5:
                print(f"  ... and {len(examples) - 5} more")
    
    # Sample of successful transformations
    if successful:
        print("\n" + "-"*70)
        print("SAMPLE SUCCESSFUL CLEANINGS")
        print("-"*70)
        for ex in successful[:10]:
            print(f"  '{ex['raw']}' → {ex['amount']}")
        if len(successful) > 10:
            print(f"  ... and {len(successful) - 10} more successful")
    
    print("\n" + "="*70 + "\n")
    
    return {
        'successful': len(successful),
        'parse_failures': dict(parse_failures)
    }


if __name__ == "__main__":
    discover_salary_patterns()