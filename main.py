"""
Main entry point for I_NEED_A_JOB data project.

Run data collection or processing pipeline.
"""

import asyncio
import sys


async def main():
    """Run collection or processing pipeline based on user input."""
    
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    else:
        print("\nWhat do you want to do?")
        print("1. collect  - Scrape SERVIR job postings")
        print("2. process  - Clean and process data")
        mode = input("\nEnter option (collect/process): ").strip().lower()
    
    if mode == "collect" or mode == "1":
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
    
    elif mode == "process" or mode == "2":
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
    
    else:
        print(f"Unknown mode: {mode}")
        print("Usage: python main.py [collect|process]")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())