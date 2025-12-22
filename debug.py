from collections import defaultdict
from servir.src.processing.parsers.vacancy_parser import transform_vacancy
from servir.src.collecting.database.queries import get_all_jobs


def discover_vacancy_patterns():
    """
    Analyze all raw vacancies to identify parsing patterns.
    
    Returns:
        dict: Statistics and examples of vacancy patterns
    """
    
    print("\n" + "="*70)
    print("VACANCY DISCOVERY AUDIT")
    print("="*70 + "\n")
    
    print("Reading vacancies from collection database...")
    jobs = get_all_jobs()
    
    if not jobs:
        print("No jobs found in collection database.")
        return None
    
    print(f"Found {len(jobs)} jobs\n")
    
    successful = []
    parse_failures = defaultdict(list)
    
    print("Analyzing vacancies (focus on parsing patterns only)...\n")
    
    for job in jobs:
        posting_id = job.get('posting_unique_id')
        vacancy_str = job.get('number_of_vacancies')
        
        result = transform_vacancy(vacancy_str)
        
        if result['vacancy_count'] is not None:
            successful.append({
                'posting_id': posting_id,
                'raw': vacancy_str,
                'count': result['vacancy_count']
            })
        else:
            parse_failures[result['error']].append({
                'posting_id': posting_id,
                'raw': vacancy_str
            })
    
    total = len(jobs)
    print("="*70)
    print("RESULTS")
    print("="*70)
    
    print(f"\nTotal vacancies analyzed: {total}")
    print(f"Successfully parsed: {len(successful)} ({100*len(successful)/total:.1f}%)")
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
        for ex in successful[:10]:
            print(f"  '{ex['raw']}' → {ex['count']}")
        if len(successful) > 10:
            print(f"  ... and {len(successful) - 10} more successful")
    
    print("\n" + "="*70 + "\n")
    
    return {
        'successful': len(successful),
        'parse_failures': dict(parse_failures)
    }


if __name__ == "__main__":
    discover_vacancy_patterns()