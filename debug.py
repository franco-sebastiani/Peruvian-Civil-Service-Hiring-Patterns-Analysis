from collections import defaultdict
from servir.src.cleaning.parsers.text_parser import transform_text
from servir.src.cleaning.database.queries import get_all_jobs


def discover_text_patterns():
    """
    Analyze all text fields to identify cleaning patterns.
    
    Tests text cleaning on all 7 text fields.
    
    Returns:
        dict: Statistics and examples of text cleaning
    """
    
    print("\n" + "="*70)
    print("TEXT CLEANING DISCOVERY AUDIT")
    print("="*70 + "\n")
    
    print("Reading text fields from collection database...")
    jobs = get_all_jobs()
    
    if not jobs:
        print("No jobs found in collection database.")
        return None
    
    print(f"Found {len(jobs)} jobs\n")
    
    # Text fields to clean
    text_fields = [
        'job_title',
        'institution',
        'experience_requirements',
        'academic_profile',
        'specialization',
        'knowledge',
        'competencies'
    ]
    
    results_by_field = {}
    
    print("Analyzing text fields (focus on cleaning patterns)...\n")
    
    for field in text_fields:
        successful = []
        parse_failures = []
        
        for job in jobs:
            text_str = job.get(field)
            result = transform_text(text_str)
            
            if result['text'] is not None:
                successful.append({
                    'raw': text_str,
                    'cleaned': result['text']
                })
            else:
                parse_failures.append({
                    'raw': text_str,
                    'error': result['error']
                })
        
        results_by_field[field] = {
            'successful': len(successful),
            'failures': len(parse_failures),
            'examples': successful[:3]
        }
    
    # Print results
    print("="*70)
    print("RESULTS BY FIELD")
    print("="*70)
    
    for field, data in results_by_field.items():
        total = data['successful'] + data['failures']
        success_rate = 100 * data['successful'] / total if total > 0 else 0
        
        print(f"\n{field}")
        print(f"  Successfully cleaned: {data['successful']}/{total} ({success_rate:.1f}%)")
        print(f"  Failures: {data['failures']}")
        
        if data['examples']:
            print(f"  Sample:")
            for ex in data['examples']:
                print(f"    Raw: '{ex['raw']}'")
                print(f"    Cleaned: '{ex['cleaned']}'")
    
    print("\n" + "="*70 + "\n")
    
    return results_by_field


if __name__ == "__main__":
    discover_text_patterns()