"""
Main entry point for I_NEED_A_JOB data project.

Run data extracting or cleaning pipeline.
"""

import asyncio
import sys


async def main():
    """Run extracting or cleaning pipeline based on user input."""
    
    if len(sys.argv) > 1:
        mode = sys.argv[1]
    else:
        print("\nWhat do you want to do?")
        print("1. extracting  - Scrape SERVIR job postings")
        print("2. cleaning  - Clean and process data")
        mode = input("\nEnter option (extracting/cleaning): ").strip().lower()

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
    
    else:
        print(f"Unknown mode: {mode}")
        print("Usage: python main.py [extracting/cleaning]")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())