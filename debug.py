from collections import defaultdict
from servir.src.processing.parsers.contract_parser import transform_contract_type
from servir.src.collecting.database.queries import get_all_jobs


def discover_contract_type_patterns():
    """
    Analyze all contract types to identify parsing patterns.
    
    Returns:
        dict: Statistics and examples of contract type parsing
    """
    
    print("\n" + "="*70)
    print("CONTRACT TYPE DISCOVERY AUDIT")
    print("="*70 + "\n")
    
    print("Reading contract types from collection database...")
    jobs = get_all_jobs()
    
    if not jobs:
        print("No jobs found in collection database.")
        return None
    
    print(f"Found {len(jobs)} jobs\n")
    
    successful = []
    parse_failures = defaultdict(list)
    
    print("Analyzing contract types (focus on parsing patterns only)...\n")
    
    for job in jobs:
        posting_id = job.get('posting_unique_id')
        contract_type_str = job.get('contract_type_raw')
        
        result = transform_contract_type(contract_type_str)
        
        if result['contract_type'] is not None:
            successful.append({
                'posting_id': posting_id,
                'raw': contract_type_str,
                'standardized': result['contract_type']
            })
        else:
            parse_failures[result['error']].append({
                'posting_id': posting_id,
                'raw': contract_type_str
            })
    
    total = len(jobs)
    print("="*70)
    print("RESULTS")
    print("="*70)
    
    print(f"\nTotal contract types analyzed: {total}")
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
            print(f"  '{ex['raw']}' → '{ex['standardized']}'")
        if len(successful) > 10:
            print(f"  ... and {len(successful) - 10} more successful")
    
    print("\n" + "="*70 + "\n")
    
    return {
        'successful': len(successful),
        'parse_failures': dict(parse_failures)
    }


if __name__ == "__main__":
    discover_contract_type_patterns()