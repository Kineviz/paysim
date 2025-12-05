import subprocess
import sys
from pathlib import Path

def run_command(command, description):
    """Run a command and print its output"""
    print(f"\n{'='*80}")
    print(f"Step: {description}")
    print(f"Running: {command}")
    print('='*80)
    
    try:
        result = subprocess.run(command, shell=True, capture_output=True, text=True)
        
        # Print output
        if result.stdout:
            print("\nOutput:")
            print(result.stdout)
            
        # Print errors if any
        if result.stderr:
            print("\nErrors:")
            print(result.stderr, file=sys.stderr)
        
        # Check return code
        if result.returncode != 0:
            print(f"\nCommand failed with return code {result.returncode}")
            return False
            
        print(f"\nCompleted: {description}")
        return True
        
    except Exception as e:
        print(f"\nError executing {command}: {e}", file=sys.stderr)
        return False

def main():
    # Define pipeline steps
    steps = [
        ("uv run src/prepare_transactions.py", "Prepare Transaction Data"),
        ("uv run src/gen_banks.py", "Extract Bank Data"),
        ("uv run src/gen_pii.py", "Generate PII Data"),
        ("uv run src/gen_relationships.py", "Generate Transaction Relationships"),
    ]
    
    # Ensure all required files exist
    required_files = [
        'data/raw/transactions.csv',
        'data/raw/clients.csv',
        'data/raw/merchants.csv'
    ]
    
    print("Checking required files...")
    for file in required_files:
        if not Path(file).exists():
            print(f"Error: Required file {file} not found!", file=sys.stderr)
            sys.exit(1)
    
    # Create output directories if they don't exist
    Path('data/processed').mkdir(parents=True, exist_ok=True)
    
    # Run each step
    for command, description in steps:
        if not run_command(command, description):
            print(f"\nData Pipeline failed at: {description}")
            sys.exit(1)
    
    print("\nData preparation completed successfully!")
    print("All processed files are available in data/processed/")

if __name__ == "__main__":
    main() 