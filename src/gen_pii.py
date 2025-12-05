import pandas as pd
import os

data_dir = os.path.join(os.path.dirname(__file__), '..', 'data')
raw_data_dir = os.path.join(data_dir, 'raw')
processed_data_dir = os.path.join(data_dir, 'processed')

def extract_pii():
    """Extract PII data from clients and create relationship CSVs"""
    # Read clients and convert column names to lowercase
    df = pd.read_csv(os.path.join(raw_data_dir, 'clients.csv'))
    df.columns = [col.lower() for col in df.columns]
    print(f"Read {len(df)} clients")
    
    # 1. Extract emails
    emails_df = pd.DataFrame({
        'id': df['email'],
        'name': df['email']  # using email as both id and value
    })
    emails_df = emails_df.drop_duplicates().sort_values('id').reset_index(drop=True)
    emails_df.to_csv(os.path.join(processed_data_dir, 'emails.csv'), index=False)
    print(f"Saved {len(emails_df)} unique emails")
    
    # 2. Extract phone numbers
    phones_df = pd.DataFrame({
        'id': df['phonenumber'],
        'name': df['phonenumber']  # using phone number as both id and value
    })
    phones_df = phones_df.drop_duplicates().sort_values('id').reset_index(drop=True)
    phones_df.to_csv(os.path.join(processed_data_dir, 'phonenumbers.csv'), index=False)
    print(f"Saved {len(phones_df)} unique phone numbers")
    
    # 3. Extract SSNs
    ssns_df = pd.DataFrame({
        'id': df['ssn'],
        'name': df['ssn']  # using SSN as both id and value
    })
    ssns_df = ssns_df.drop_duplicates().sort_values('id').reset_index(drop=True)
    ssns_df.to_csv(os.path.join(processed_data_dir, 'ssns.csv'), index=False)
    print(f"Saved {len(ssns_df)} unique SSNs")
    
    # Create relationship CSVs
    # 1. Has_Email relationships
    has_email = df[['id', 'email']].copy()
    has_email.columns = ['client_id', 'email_id']
    has_email['client_id'] = has_email['client_id'].astype('string')
    has_email['email_id'] = has_email['email_id'].astype('string')
    #sort by id
    has_email = has_email.sort_values(by=['client_id', 'email_id']).reset_index(drop=True)
    has_email.to_csv(os.path.join(processed_data_dir, 'Has_Email.csv'), index=False)
    print(f"Saved {len(has_email)} Has_Email relationships")
    
    # 2. Has_Phonenumber relationships
    has_phone = df[['id', 'phonenumber']].copy()
    has_phone.columns = ['client_id', 'phonenumber_id']
    has_phone['client_id'] = has_phone['client_id'].astype('string')
    has_phone['phonenumber_id'] = has_phone['phonenumber_id'].astype('string')
    #sort by id
    has_phone = has_phone.sort_values(by=['client_id', 'phonenumber_id']).reset_index(drop=True)
    has_phone.to_csv(os.path.join(processed_data_dir, 'Has_Phonenumber.csv'), index=False)
    print(f"Saved {len(has_phone)} Has_Phonenumber relationships")
    
    # 3. Has_SSN relationships
    has_ssn = df[['id', 'ssn']].copy()
    has_ssn.columns = ['client_id', 'ssn_id']
    has_ssn['client_id'] = has_ssn['client_id'].astype('string')
    has_ssn['ssn_id'] = has_ssn['ssn_id'].astype('string')
    #sort by id
    has_ssn = has_ssn.sort_values(by=['client_id', 'ssn_id']).reset_index(drop=True)
    has_ssn.to_csv(os.path.join(processed_data_dir, 'Has_SSN.csv'), index=False)
    print(f"Saved {len(has_ssn)} Has_SSN relationships")
    
    # Print samples of all generated files
    print("\nSamples of generated PII files:")
    for file_name in ['emails.csv', 'phonenumbers.csv', 'ssns.csv']:
        print(f"\n{file_name}:")
        df_sample = pd.read_csv(os.path.join(processed_data_dir, file_name))
        print(df_sample.head())
    
    print("\nSamples of generated relationship files:")
    for file_name in ['Has_Email.csv', 'Has_Phonenumber.csv', 'Has_SSN.csv']:
        print(f"\n{file_name}:")
        df_sample = pd.read_csv(os.path.join(processed_data_dir, file_name))
        print(df_sample.head())

if __name__ == "__main__":
    extract_pii() 