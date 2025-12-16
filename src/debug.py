import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from extractors.extractor_servir import fetch_job_offer

async def main():
    """Loop through all 10 job offers on the first page."""
    
    print("\n" + "=" * 60)
    print("Testing extraction on first page (10 job offers)")
    print("=" * 60 + "\n")
    
    for job_index in range(10):
        print(f"\n{'='*60}")
        print(f"Extracting job offer {job_index + 1}/10")
        print(f"{'='*60}")
        await fetch_job_offer(job_offer_index=job_index)


if __name__ == "__main__":
    asyncio.run(main())