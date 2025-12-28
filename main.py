"""
Main entry point for I_NEED_A_JOB data project.

Run data pipeline: extracting → cleaning → transforming → analyzing
"""

import asyncio
import sys


async def main():
    """Run pipeline phase based on user input."""
    
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    else:
        print("\n" + "="*70)
        print("I_NEED_A_JOB DATA PIPELINE")
        print("="*70)
        print("\nWhat phase do you want to run?")
        print("1. extracting   - Scrape SERVIR job postings")
        print("2. cleaning     - Clean and standardize data")
        print("3. transforming - Transform into analysis-ready format")
        print("4. analyzing    - Analyze data (future phase)")
        print("="*70)
        mode = input("\nEnter option (extracting/cleaning/transforming/analyzing): ").strip().lower()

    if mode == "extracting" or mode == "1":
        from servir.src.extracting.pipeline.orchestrator import collect_all_servir_jobs
        try:
            await collect_all_servir_jobs()
        except KeyboardInterrupt:
            print("\n\nInterrupted by user.")
            sys.exit(0)
        except Exception as e:
            print(f"\nError: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    elif mode == "cleaning" or mode == "2":
        from servir.src.cleaning.pipeline.orchestrator import clean_all_jobs
        try:
            clean_all_jobs()
        except KeyboardInterrupt:
            print("\n\nInterrupted by user.")
            sys.exit(0)
        except Exception as e:
            print(f"\nError: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    elif mode == "transforming" or mode == "3":
        print("\n" + "="*70)
        print("TRANSFORMING PHASE - NOT YET IMPLEMENTED")
        print("="*70)
        print("\nThis phase will:")
        print("- Split contract_type into type + temporal_nature")
        print("- Split experience into general + specific")
        print("- Split academic into level + field")
        print("- Convert knowledge/competencies/specialization to arrays")
        print("- Create institution dimension table")
        print("="*70 + "\n")
        sys.exit(0)
    
    elif mode == "analyzing" or mode == "4":
        print("\n" + "="*70)
        print("ANALYZING PHASE - NOT YET IMPLEMENTED")
        print("="*70)
        print("\nThis phase will:")
        print("- Detect institutional anomalies")
        print("- Identify temporal requirement changes")
        print("- Statistical hypothesis testing")
        print("- Generate reports and visualizations")
        print("="*70 + "\n")
        sys.exit(0)
    
    else:
        print(f"\nUnknown mode: {mode}")
        print("Usage: python main.py [extracting/cleaning/transforming/analyzing]")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())