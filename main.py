"""
Main entry point for I_NEED_A_JOB data project.

Run data collection or processing pipeline.
"""

import asyncio
import sys


async def main():
    """Run collection or processing pipeline based on argument."""
    
    mode = sys.argv[1] if len(sys.argv) > 1 else "process"
    
    if mode == "collect":
        from servir.src.collecting.pipeline.orchestrator import collect_all_servir_jobs
        try:
            await collect_all_servir_jobs()
        except KeyboardInterrupt:
            print("\n\nInterrupted by user.")
            sys.exit(0)
        except Exception as e:
            print(f"\n  Error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    elif mode == "process":
        from servir.src.processing.pipeline.orchestrator import process_all_jobs
        try:
            process_all_jobs()
        except KeyboardInterrupt:
            print("\n\nInterrupted by user.")
            sys.exit(0)
        except Exception as e:
            print(f"\n  Error: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    else:
        print(f"Unknown mode: {mode}")
        print("Usage: python main.py [collect|process]")
        print("  collect - Run SERVIR data collection")
        print("  process - Run data cleaning and processing")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())