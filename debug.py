"""
Salary discovery script for data quality audit.

Analyzes raw salaries from collection database to find parsing patterns.
Identifies which salaries can be transformed and which need special handling.
"""

from collections import defaultdict
from servir.src.processing.transformers.salary_transform import transform_salary
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
    invalid_range = defaultdict(list)   # Group by validation error
    
    # Process each job's salary
    print("Analyzing salaries...\n")
    
    for job in jobs:
        posting_id = job.get('posting_unique_id')
        salary_str = job.get('monthly_salary')
        
        # Try to transform
        result = transform_salary(salary_str)
        
        if result['is_valid']:
            successful.append({
                'posting_id': posting_id,
                'raw': salary_str,
                'amount': result['salary_amount']
            })
        elif 'parse' in result['error'].lower():
            parse_failures[result['error']].append({
                'posting_id': posting_id,
                'raw': salary_str
            })
        else:
            invalid_range[result['error']].append({
                'posting_id': posting_id,
                'raw': salary_str,
                'amount': result['salary_amount']
            })
    
    # Print report
    print("="*70)
    print("RESULTS")
    print("="*70)
    
    total = len(jobs)
    print(f"\nTotal salaries analyzed: {total}")
    print(f"Successfully transformed: {len(successful)} ({100*len(successful)/total:.1f}%)")
    print(f"Parse failures: {sum(len(v) for v in parse_failures.values())}")
    print(f"Invalid range: {sum(len(v) for v in invalid_range.values())}")
    
    # Parse failures detail
    if parse_failures:
        print("\n" + "-"*70)
        print("PARSE FAILURES (Could not extract number)")
        print("-"*70)
        for error, examples in parse_failures.items():
            print(f"\n{error} ({len(examples)} cases)")
            for ex in examples[:5]:
                print(f"  ID: {ex['posting_id']}, Raw: {ex['raw']}")
            if len(examples) > 5:
                print(f"  ... and {len(examples) - 5} more")
    
    # Invalid range detail
    if invalid_range:
        print("\n" + "-"*70)
        print("INVALID RANGE (Outside bounds)")
        print("-"*70)
        for error, examples in invalid_range.items():
            print(f"\n{error} ({len(examples)} cases)")
            for ex in examples[:5]:
                print(f"  ID: {ex['posting_id']}, Raw: {ex['raw']}, Amount: {ex['amount']}")
            if len(examples) > 5:
                print(f"  ... and {len(examples) - 5} more")
    
    # Sample of successful transformations
    if successful:
        print("\n" + "-"*70)
        print("SAMPLE SUCCESSFUL TRANSFORMATIONS")
        print("-"*70)
        for ex in successful[:10]:
            print(f"  {ex['raw']} → {ex['amount']}")
        if len(successful) > 10:
            print(f"  ... and {len(successful) - 10} more successful")
    
    print("\n" + "="*70 + "\n")
    
    return {
        'successful': len(successful),
        'parse_failures': dict(parse_failures),
        'invalid_range': dict(invalid_range)
    }


if __name__ == "__main__":
    discover_salary_patterns()