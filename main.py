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
        print("\nNote: Analysis is done in Jupyter notebooks (see notebooks/ folder)")
        print("="*70)
        mode = input("\nEnter option (extracting/cleaning/transforming): ").strip().lower()

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
        from servir.src.transforming.pipeline.orchestrator import TransformationOrchestrator
        from pathlib import Path
        
        try:
            print("\n" + "="*70)
            print("TRANSFORMING PHASE")
            print("="*70)
            
            # Setup paths
            project_root = Path(__file__).resolve().parent
            
            cleaned_db = project_root / "servir" / "data" / "cleaned" / "servir_jobs_cleaned.db"
            transformed_db = project_root / "servir" / "data" / "transformed" / "servir_jobs_transformed.db"
            
            matches_dbs = {
                'job_title': project_root / "servir" / "data" / "transformed" / "job_title" / "job_title_matches.db",
                'institution_name': project_root / "servir" / "data" / "transformed" / "institution_name" / "institution_name_matches.db",
                'academic': project_root / "servir" / "data" / "transformed" / "academic" / "academic_matches.db"
            }
            
            # Check prerequisites
            print("\nChecking match databases...")
            missing = []
            for name, path in matches_dbs.items():
                exists = path.exists()
                print(f"  {name}: {'✓' if exists else '✗'}")
                if not exists:
                    missing.append(name)
            
            if missing:
                print(f"\n✗ Missing match databases: {', '.join(missing)}")
                print("\nRun the matchers first:")
                for name in missing:
                    print(f"  python servir/src/transforming/transformers/{name}/{name}_matcher.py")
                sys.exit(1)
            
            print("\n✓ All match databases found")
            
            # Run orchestrator
            orchestrator = TransformationOrchestrator(cleaned_db, transformed_db, matches_dbs)
            orchestrator.run()
            
        except KeyboardInterrupt:
            print("\n\nInterrupted by user.")
            sys.exit(0)
        except Exception as e:
            print(f"\nError: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)
    
    else:
        print(f"\nUnknown mode: {mode}")
        print("Usage: python main.py [extracting/cleaning/transforming]")
        print("\nNote: Analysis is done in Jupyter notebooks (see notebooks/ folder)")
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(main())