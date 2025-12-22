from collections import defaultdict
from servir.src.processing.parsers.date_parser import transform_date
from servir.src.collecting.database.queries import get_all_jobs


def discover_date_patterns():
    """
    Analyze all raw dates to identify parsing patterns.
    
    Returns:
        dict: Statistics and examples of date patterns
    """
    
    print("\n" + "="*70)
    print("DATE DISCOVERY AUDIT")
    print("="*70 + "\n")
    
    print("Reading dates from collection database...")
    jobs = get_all_jobs()
    
    if not jobs:
        print("No jobs found in collection database.")
        return None
    
    print(f"Found {len(jobs)} jobs\n")
    
    successful = []
    parse_failures = defaultdict(list)
    
    print("Analyzing dates (focus on parsing patterns only)...\n")
    
    for job in jobs:
        posting_id = job.get('posting_unique_id')
        start_date_str = job.get('posting_start_date')
        end_date_str = job.get('posting_end_date')
        
        # Parse start date
        start_result = transform_date(start_date_str)
        # Parse end date
        end_result = transform_date(end_date_str)
        
        if start_result['date_iso'] is not None and end_result['date_iso'] is not None:
            successful.append({
                'posting_id': posting_id,
                'start_raw': start_date_str,
                'start_iso': start_result['date_iso'],
                'end_raw': end_date_str,
                'end_iso': end_result['date_iso']
            })
        else:
            if start_result['date_iso'] is None:
                parse_failures[f"Start: {start_result['error']}"].append({
                    'posting_id': posting_id,
                    'raw': start_date_str
                })
            if end_result['date_iso'] is None:
                parse_failures[f"End: {end_result['error']}"].append({
                    'posting_id': posting_id,
                    'raw': end_date_str
                })
    
    total = len(jobs)
    print("="*70)
    print("RESULTS")
    print("="*70)
    
    print(f"\nTotal dates analyzed: {total * 2}")
    print(f"Successfully parsed: {len(successful) * 2} ({100*len(successful)/total:.1f}%)")
    print(f"Parse failures: {sum(len(v) for v in parse_failures.values())}")
    
    if parse_failures:
        print("\n" + "-"*70)
        print("PARSE FAILURES (Parsing didn't work)")
        print("-"*70)
        for error, examples in parse_failures.items():
            print(f"\n{error} ({len(examples)} cases)")
            for ex in examples[:5]:
                print(f"  ID: {ex['posting_id']}, Raw: '{ex['raw']}'")
            if len(examples) > 5:
                print(f"  ... and {len(examples) - 5} more")
    
    if successful:
        print("\n" + "-"*70)
        print("SAMPLE SUCCESSFUL PARSINGS")
        print("-"*70)
        for ex in successful[:5]:
            print(f"  Start: '{ex['start_raw']}' → {ex['start_iso']}")
            print(f"  End:   '{ex['end_raw']}' → {ex['end_iso']}")
        if len(successful) > 5:
            print(f"  ... and {len(successful) - 5} more successful")
    
    print("\n" + "="*70 + "\n")
    
    return {
        'successful': len(successful),
        'parse_failures': dict(parse_failures)
    }


if __name__ == "__main__":
    discover_date_patterns()