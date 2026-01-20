"""
Debug/run file for labeling phase.

Choose which function to run via command-line menu.
"""

import subprocess
import sys
from servir.src.transforming.labeling.sampler import main as sample_and_save


def run_labeling_app():
    """Launch the Streamlit labeling app."""
    print("\nLaunching Streamlit labeling app...")
    subprocess.run(["streamlit", "run", "servir/src/transforming/labeling/app.py"])


if __name__ == "__main__":
    if len(sys.argv) > 1:
        choice = sys.argv[1]
    else:
        print("\n" + "="*70)
        print("LABELING PHASE DEBUG MENU")
        print("="*70)
        print("\nWhat do you want to run?")
        print("1. sampler - Extract samples from cleaned database")
        print("2. app     - Launch Streamlit labeling interface")
        print("="*70)
        choice = input("\nEnter option (1/2 or sampler/app): ").strip().lower()
    
    if choice in ["1", "sampler"]:
        sample_and_save()
    elif choice in ["2", "app"]:
        run_labeling_app()
    else:
        print(f"\nUnknown option: {choice}")
        print("Usage: python debug.py [sampler/app]")
        sys.exit(1)