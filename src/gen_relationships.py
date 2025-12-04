import pandas as pd
import os

data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')

def generate_relationships():
    """Generate relationship CSVs from transactions data"""
    # Read transactions
    df = pd.read_csv(os.path.join(data_dir, 'transactions_cleaned.csv'))
    print(f"Read {len(df)} transactions")
    
    # 1. Client_Perform_Transaction (all transactions originated by clients)
    client_perform = df[['idorig', 'globalstep', 'timestamp']].copy()
    client_perform.columns = ['client_id', 'transaction_id', 'timestamp']
    client_perform['client_id'] = client_perform['client_id'].astype('string')
    client_perform['transaction_id'] = client_perform['transaction_id'].astype('string')
    #sort by id
    client_perform = client_perform.sort_values(by=['client_id', 'transaction_id']).reset_index(drop=True)
    client_perform.to_csv(os.path.join(data_dir, 'Client_Perform_Transaction.csv'), index=False)
    print(f"Saved {len(client_perform)} Client_Perform_Transaction relationships")
    
    # 2. Transaction_To_Client (transactions destined to clients)
    trans_to_client = df[df['typedest'].isin(['CLIENT', 'MULE'])][['globalstep', 'iddest', 'timestamp']].copy()
    trans_to_client.columns = ['transaction_id', 'client_id', 'timestamp']
    trans_to_client['client_id'] = trans_to_client['client_id'].astype('string')
    trans_to_client['transaction_id'] = trans_to_client['transaction_id'].astype('string')
    #sort by id
    trans_to_client = trans_to_client.sort_values(by=['transaction_id', 'client_id']).reset_index(drop=True)
    trans_to_client.to_csv(os.path.join(data_dir, 'Transaction_To_Client.csv'), index=False)
    print(f"Saved {len(trans_to_client)} Transaction_To_Client relationships")
    
    # 3. Transaction_To_Merchant (transactions destined to merchants)
    trans_to_merchant = df[df['typedest'] == 'MERCHANT'][['globalstep', 'iddest', 'timestamp']].copy()
    trans_to_merchant.columns = ['transaction_id', 'merchant_id', 'timestamp']
    trans_to_merchant['merchant_id'] = trans_to_merchant['merchant_id'].astype('string')
    trans_to_merchant['transaction_id'] = trans_to_merchant['transaction_id'].astype('string')
    #sort by id
    trans_to_merchant = trans_to_merchant.sort_values(by=['transaction_id', 'merchant_id']).reset_index(drop=True)
    trans_to_merchant.to_csv(os.path.join(data_dir, 'Transaction_To_Merchant.csv'), index=False)
    print(f"Saved {len(trans_to_merchant)} Transaction_To_Merchant relationships")
    
    # 4. Transaction_To_Bank (transactions destined to banks)
    trans_to_bank = df[df['typedest'] == 'BANK'][['globalstep', 'iddest', 'timestamp']].copy()
    trans_to_bank.columns = ['transaction_id', 'bank_id', 'timestamp']
    trans_to_bank['bank_id'] = trans_to_bank['bank_id'].astype('string')
    trans_to_bank['transaction_id'] = trans_to_bank['transaction_id'].astype('string')
    #sort by id
    trans_to_bank = trans_to_bank.sort_values(by=['transaction_id', 'bank_id']).reset_index(drop=True)
    trans_to_bank.to_csv(os.path.join(data_dir, 'Transaction_To_Bank.csv'), index=False)
    print(f"Saved {len(trans_to_bank)} Transaction_To_Bank relationships")
    
    # Print sample of each relationship file
    print("\nSamples of generated relationship files:")
    for file_name in [
        'Client_Perform_Transaction.csv',
        'Transaction_To_Client.csv',
        'Transaction_To_Merchant.csv',
        'Transaction_To_Bank.csv'
    ]:
        print(f"\n{file_name}:")
        df_sample = pd.read_csv(os.path.join(data_dir, file_name))
        print(df_sample.head())

if __name__ == "__main__":
    generate_relationships() 