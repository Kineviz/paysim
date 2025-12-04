import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')

def prepare_transactions(input_file, output_file):
    """
    Prepare transaction data:
    1. Convert column names to lowercase
    2. Generate timestamp from globalstep
    """
    print(f"Reading {input_file}...")
    df = pd.read_csv(input_file)
    
    # Convert column names to lowercase
    df.columns = [col.lower() for col in df.columns]
    print("Converted column names to lowercase")
    
    # Sort by globalstep to ensure timestamps are sequential
    # df = df.sort_values('globalstep')
    
    # Generate timestamps
    start_time = datetime(2024, 1, 1)  # Start from January 1st, 2024
    timestamps = []
    current_time = start_time
    
    print("Generating timestamps...")
    for _ in range(len(df)):
        timestamps.append(current_time.strftime('%Y-%m-%dT%H:%M:%S'))
        # Random increment between 1 and 30 seconds
        increment = random.randint(1, 30)
        current_time += timedelta(seconds=increment)
    
    # Add timestamp column
    df['timestamp'] = timestamps
    df['amount'] = df['amount'].round(2)  # Round amount to 2 decimal places
    
    # Save to output file
    print(f"Saving to {output_file}...")
    # sort by globalstep before saving
    df = df.sort_values(by=['globalstep']).reset_index(drop=True)
    df.to_csv(output_file, index=False)
    print("Done!")
    
    # Print sample
    print("\nSample of prepared data:")
    print(df[['globalstep', 'timestamp']].head())

def main():
    input_file = os.path.join(data_dir,'transactions.csv')
    output_file = os.path.join(data_dir, 'transactions_cleaned.csv')
    
    try:
        prepare_transactions(input_file, output_file)
    except Exception as e:
        print(f"Error: {e}")
        raise

if __name__ == "__main__":
    main() 