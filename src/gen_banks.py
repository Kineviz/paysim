import pandas as pd
import numpy as np
import os

data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
raw_data_dir = os.path.join(data_dir, 'raw')
processed_data_dir = os.path.join(data_dir, 'processed')

def generate_bank_id():
    """Generate bank ID in format XX-XXXXXXX where X are digits"""
    return f"{np.random.randint(10,100):02d}-{np.random.randint(1000000,10000000):07d}"
def extract_banks():
    """Extract unique banks from transactions and create banks.csv"""
    # Read transactions
    df = pd.read_csv(os.path.join(raw_data_dir, 'transactions.csv'))
    
    # Find all rows where typedest or typeorig is 'BANK'
    bank_data = []
    
    # Extract from destination
    bank_mask = df['typedest'] == 'BANK'
    bank_data.extend(zip(df[bank_mask]['iddest'], df[bank_mask]['namedest']))
    
    # Extract from origin
    bank_mask = df['typeorig'] == 'BANK'
    bank_data.extend(zip(df[bank_mask]['idorig'], df[bank_mask]['nameorig']))
    
    # Convert to set of tuples to get unique combinations
    bank_data = set(bank_data)
    print(f"Found {len(bank_data)} unique banks")
    
    # Create banks dataframe with just id and name
    banks_df = pd.DataFrame(bank_data, columns=['id', 'name'])
    
    # Sort by ID
    banks_df = banks_df.sort_values(by='id').reset_index(drop=True)
    # Save to CSV
    output_path = os.path.join(processed_data_dir, 'banks.csv')
    banks_df.to_csv(output_path, index=False)
    print("\nSample of banks:")
    print(banks_df.head())
    print(f"\nSaved {len(banks_df)} banks to {output_path}")

if __name__ == "__main__":
    extract_banks() 