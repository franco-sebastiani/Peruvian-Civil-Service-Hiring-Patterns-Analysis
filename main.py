"""
Main entry point for I_NEED_A_JOB data collection project.

Provides interface to run different operations:
- Full SERVIR data collection
- Testing/debugging
- Database queries
"""

import asyncio
import sys
from pathlib import Path


async def run_servir_pipeline():
    """Run the complete SERVIR data collection pipeline."""
    from servir.src.pipeline import collect_all_servir_jobs
    
    print("\n" + "="*70)
    print("Running SERVIR Data Collection Pipeline")
    print("="*70)
    
    stats = await collect_all_servir_jobs()
    
    return stats


async def run_pagination_test():
    """Run pagination test to verify all pages are accessible."""
    from debug import test_all_pages_pagination
    
    print("\nRunning pagination test...")
    await test_all_pages_pagination()


def show_menu():
    """Display main menu and get user choice."""
    print("\n" + "="*70)
    print("I_NEED_A_JOB - Data Collection System")
    print("="*70)
    print("\nWhat would you like to do?\n")
    print("1. Run SERVIR data collection pipeline (full)")
    print("2. Run pagination test (verify pages work)")
    print("3. Exit")
    print("\nEnter your choice (1-3): ", end="")
    
    choice = input().strip()
    return choice


async def main():
    """Main program flow."""
    while True:
        choice = show_menu()
        
        if choice == "1":
            print("\n⚠ This will scrape ~2,633 jobs and save to database.")
            print("Estimated time: ~75 minutes")
            confirm = input("Continue? (yes/no): ").strip().lower()
            
            if confirm == "yes":
                await run_servir_pipeline()
            else:
                print("Cancelled.")
        
        elif choice == "2":
            await run_pagination_test()
        
        elif choice == "3":
            print("\nExiting...")
            sys.exit(0)
        
        else:
            print("\n✗ Invalid choice. Please enter 1, 2, or 3.")
        
        # Ask if user wants to do something else
        print("\n" + "-"*70)
        again = input("Do you want to do something else? (yes/no): ").strip().lower()
        if again != "yes":
            print("Goodbye!")
            break


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)