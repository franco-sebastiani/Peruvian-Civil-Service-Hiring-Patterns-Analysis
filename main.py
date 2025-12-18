"""
Main entry point for I_NEED_A_JOB data collection project.

Simple interface to run the data collection pipeline.
"""

import asyncio
import sys


async def main():
    """Run the SERVIR data collection pipeline."""
    from servir.src.pipeline import collect_all_servir_jobs
    
    try:
        await collect_all_servir_jobs()
    except KeyboardInterrupt:
        print("\n\nInterrupted by user.")
        sys.exit(0)
    except Exception as e:
        print(f"\n✗ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())