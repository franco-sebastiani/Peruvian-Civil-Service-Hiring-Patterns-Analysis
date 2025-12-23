from collections import defaultdict
from servir.src.processing.parsers.date_parser import transform_date
from servir.src.collecting.database.queries import get_all_jobs


def discover_contract_type_patterns():
    """
    Analyze all unique contract types from raw data.
    
    Returns:
        dict: All unique contract types and their frequencies
    """
    from servir.src.collecting.database.queries import get_all_jobs
    from collections import Counter
    
    jobs = get_all_jobs()
    
    print("\n" + "="*70)
    print("CONTRACT TYPE PATTERNS")
    print("="*70 + "\n")
    
    # Collect all contract types
    contract_types = [job.get('contract_type_raw') for job in jobs]
    
    # Count frequencies
    counter = Counter(contract_types)
    
    print(f"Total unique contract types: {len(counter)}\n")
    
    # Sort by frequency (most common first)
    for contract_type, count in counter.most_common():
        percentage = 100 * count / len(jobs)
        print(f"{count:4d} ({percentage:5.1f}%) - {contract_type}")
    
    print("\n" + "="*70 + "\n")


if __name__ == "__main__":
    discover_contract_type_patterns()