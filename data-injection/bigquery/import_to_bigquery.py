# Import required libraries
import sys
import pandas as pd 
import os
from google.cloud import bigquery
from google.oauth2 import service_account

from dotenv import load_dotenv

#automatically load .env file
load_dotenv()

# Load configuration
datasetName = os.getenv('DATASET_NAME') or "paysim_graph"
graphName = os.getenv('GRAPH_NAME') or "graph_view"
google_auth_keyfile = os.getenv('GOOGLE_AUTH_KEYFILE') or 'google_auth_keyfile.json'

data_dir = os.path.join(os.path.dirname(__file__), './../../', 'data')
raw_data_dir = os.path.join(data_dir, 'raw')
processed_data_dir = os.path.join(data_dir, 'processed')

def create_graph(client):
    """Execute the property creation SQL using BigQuery client"""
    try:

        sql_path =  os.path.join(os.path.dirname(__file__), 'graph_view.sql')
        if not os.path.exists(sql_path):
            raise FileNotFoundError(f"No file found: {sql_path}")

        sql = '' 
        with open(sql_path, 'r', encoding='utf-8', newline='\n') as file:
            sql = file.read()

        ## replace DATASET paysim_graph name to the variable
        sql = sql.replace('paysim_graph.', f'{datasetName}.')

        ## replace GRAPH graph_view name to the variable
        sql = sql.replace('.graph_view', f'.{graphName}')

        ## remove last ;
        if sql.strip().endswith(';'):
            sql = sql.strip()[:-1]
        
        # Run the query
        job = client.query(sql)
        job.result()  # Wait for query to complete
        print("Successfully created property graph")
    except Exception as e:
        print(f"Error creating property graph: {e}", file=sys.stderr)
        raise e

def delete_all_tables(client, dataset_id):
    """Delete all tables in the specified dataset"""
    try:
        tables = client.list_tables(dataset_id)
        for table in tables:
            client.delete_table(table.reference)
            print(f"Deleted table {table.table_id}")
    except Exception as e:
        print(f"Error deleting tables: {e}")

def create_dataset(client, dataset_name):
    """Create a new dataset in BigQuery"""
    dataset_id = f"{client.project}.{dataset_name}"
    dataset = bigquery.Dataset(dataset_id)
    dataset.location = "US"
    
    try:
        dataset = client.create_dataset(dataset, exists_ok=True)
        print(f"Dataset {dataset_id} created successfully")
    except Exception as e:
        print(f"Error creating dataset: {e}")
    
    return dataset_id

def prepare_data(csv_file, is_transaction=False):
    """Prepare data by normalizing column names and creating IDs"""
    try:
        # Determine which directory to read from
        # Original files (clients.csv, merchants.csv) are in raw/
        # All processed files are in processed/
        if csv_file in ['clients.csv', 'merchants.csv']:
            file_path = os.path.join(raw_data_dir, csv_file)
        else:
            file_path = os.path.join(processed_data_dir, csv_file)
        
        # Read CSV file
        df = pd.read_csv(file_path)
        print(f"Read {len(df)} rows from {csv_file}")
        
        # Convert column names to lowercase
        df.columns = [col.lower() for col in df.columns]
        
        # Check if this is a relationship table
        is_relationship = any(csv_file.startswith(prefix) for prefix in 
                            ['Has_', 'Client_Perform_', 'Transaction_To_'])
        
        # Handle IDs based on table type
        if is_relationship:
            # For relationship tables, convert all *_id columns to string
            id_columns = [col for col in df.columns if col.endswith('_id')]
            for col in id_columns:
                df[col] = df[col].astype('string')
        elif is_transaction:
            # For transactions, convert globalstep to string id
            df['id'] = df['globalstep'].astype('string')
        else:
            # For other entity tables, ensure id exists and convert to string
            if 'id' not in df.columns:
                raise ValueError(f"No 'id' column found in {csv_file}")
            df['id'] = df['id'].astype('string')
        
        print(f"Prepared data columns: {', '.join(df.columns)}")
        if not is_relationship:
            print(f"ID column type: {df['id'].dtype}")
        else:
            print(f"ID column types: {', '.join(f'{col}: {df[col].dtype}' for col in id_columns)}")
        return df
        
    except Exception as e:
        print(f"Error preparing data from {csv_file}: {e}")
        return None

def load_csv_to_bigquery(client, dataset_id, df, table_name):
    """Load a DataFrame into a BigQuery table"""
    try:
        if df is None:
            raise ValueError("DataFrame is None")
            
        # Define the destination table
        table_id = f"{dataset_id}.{table_name}"
        
        # Check if this is a relationship table
        is_relationship = any(table_name.startswith(prefix) for prefix in 
                            ['Has_', 'Client_Perform_', 'Transaction_To_'])
        
        # Configure the load job
        if is_relationship:
            # For relationship tables, set all ID columns to STRING
            schema = []
            for col in df.columns:
                if col.endswith('_id'):
                    # ID columns should be STRING
                    schema.append(bigquery.SchemaField(col, "STRING", mode="REQUIRED"))
                else:
                    # Let BigQuery autodetect other columns
                    schema.append(bigquery.SchemaField(col, "STRING" if col == "timestamp" else "FLOAT"))
            
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                write_disposition="WRITE_TRUNCATE"
            )
        else:
            # For entity tables, set id as primary key
            schema = [
                bigquery.SchemaField("id", "STRING", mode="REQUIRED"),
            ]
            job_config = bigquery.LoadJobConfig(
                schema=schema,
                autodetect=True,
                write_disposition="WRITE_TRUNCATE",
                clustering_fields=['id']
            )
        
        # Load DataFrame to BigQuery
        job = client.load_table_from_dataframe(
            df, 
            table_id,
            job_config=job_config
        )
        
        # Wait for job to complete
        job.result()
        
        # Add primary key constraint only for entity tables
        if not is_relationship:
            query = f"""
            ALTER TABLE `{table_id}`
            ADD PRIMARY KEY(id) NOT ENFORCED
            """
            client.query(query).result()
            print(f"Added primary key constraint on id for {table_name}")
        
        # Get and print table info
        table = client.get_table(table_id)
        print(f"Loaded {table.num_rows} rows into {table_id}")
        print(f"Table schema for {table_name}:")
        for field in table.schema:
            print(f"  {field.name}: {field.field_type}")
            
    except Exception as e:
        print(f"Error loading data to {table_name}: {e}")
        raise

def main():
    # Initialize BigQuery client
    credentials = service_account.Credentials.from_service_account_file(
        os.path.join(os.path.dirname(__file__), google_auth_keyfile)
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    # Create dataset with correct name
    print(f"\n1. Creating dataset '{datasetName}'...")
    dataset_id = create_dataset(client, datasetName)

    # Delete all existing tables
    print(f"\n2. Deleting existing tables in dataset '{datasetName}'...")
    delete_all_tables(client, dataset_id)

    # Define the files to load
    files_to_load = [
        # Entity tables
        ("clients.csv", "Client", False),
        ("merchants.csv", "Merchant", False),
        ("banks.csv", "Bank", False),
        ("transactions_cleaned.csv", "Transaction", True),
        ("emails.csv", "Email", False),
        ("phonenumbers.csv", "PhoneNumber", False),
        ("ssns.csv", "SSN", False),
        
        # Relationship tables
        ("Client_Perform_Transaction.csv", "Client_Perform_Transaction", False),
        ("Transaction_To_Client.csv", "Transaction_To_Client", False),
        ("Transaction_To_Merchant.csv", "Transaction_To_Merchant", False),
        ("Transaction_To_Bank.csv", "Transaction_To_Bank", False),
        ("Has_Email.csv", "Has_Email", False),
        ("Has_Phonenumber.csv", "Has_PhoneNumber", False),
        ("Has_SSN.csv", "Has_SSN", False)
    ]

    print(f"\n3. Processing and loading data files into dataset '{datasetName}'...")

    # Process and load all files
    for csv_file, table_name, is_transaction in files_to_load:
        print(f"\nProcessing {csv_file} -> {table_name}")
        # Prepare the data
        df = prepare_data(csv_file, is_transaction)
        if df is not None:
            # Load to BigQuery
            load_csv_to_bigquery(client, dataset_id, df, table_name)

    print(f"\n4. Creating property graph view '{graphName}' in dataset '{datasetName}'...")
    create_graph(client)

    print("\nData import to BigQuery completed.")

if __name__ == "__main__":
    main()