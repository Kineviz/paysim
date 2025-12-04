# Recreated clean file with English translations for comments
# Import required libraries
import pandas as pd
import json
import math
import datetime
import decimal
import random
import string
import os
from google.cloud import spanner
from google.oauth2 import service_account

instanceName = "demo-2025"
databaseName = "paysim_schema_less"
graphName = "paysim_schema_less_graph"

data_dir = os.path.join(os.path.dirname(__file__), './../../', 'data')

def get_spanner_client():
    try:
        print("Initializing Spanner client...")
        credentials = service_account.Credentials.from_service_account_file(
            os.path.join(os.path.dirname(__file__), 'google_auth_keyfile.json')
        )
        client = spanner.Client(credentials=credentials)
        print(f"Connected to GCP project: {client.project}")
        return client
    except Exception as e:
        print(f"Error initializing Spanner client: {e}")
        raise e


def delete_all_tables(database):
    """Delete all tables in the specified database"""
    try:
        # try to delete property graph first
        operation = database.update_ddl([f'''DROP PROPERTY GRAPH IF EXISTS `{graphName}`'''])
        operation.result()
        print("Deleted property graph if it existed")

        # First get and delete all views
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                "SELECT table_name FROM information_schema.views WHERE table_schema = ''"
            )
            views = [row[0] for row in results]

        if views:
            print(f"Deleting views: {', '.join(views)}")
            # Construct DDL statements to drop views
            ddl_statements = [f"DROP VIEW {view}" for view in views]

            # Execute DDL statements to drop views
            try:
                operation = database.update_ddl(ddl_statements)
                operation.result()
                print(f"Deleted views: {', '.join(views)}")
            except Exception as e:
                print(f"Error deleting views: {e}")
                # Continue trying to delete tables

        # Get all table names
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                "SELECT table_name FROM information_schema.tables WHERE table_schema = ''"
            )
            tables = [row[0] for row in results]

        if not tables:
            print("No tables to delete")
            return database

        # Construct DDL statements to drop tables
        ddl_statements = [f"DROP TABLE {table}" for table in tables]

        # Execute DDL statements to drop tables
        operation = database.update_ddl(ddl_statements)
        operation.result()

        print(f"Deleted tables: {', '.join(tables)}")
        return database
    except Exception as e:
        print(f"Error deleting tables: {e}")
        return database


def create_database(spanner_client, instance_name, database_name):
    """Create a new instance and database in Spanner"""
    try:
        instance_id = instance_name
        database_id = database_name
        # Check if instance exists
        try:
            instance = spanner_client.instance(instance_id)
            instance.reload()
            print(f"Using existing instance {instance_id}")
        except Exception:
            # Create correctly formatted instance config path
            project_id = spanner_client.project
            instance_config = f"projects/{project_id}/instanceConfigs/regional-us-central1"
            instance = spanner_client.instance(
                instance_id,
                configuration_name=instance_config,
                display_name=f"{instance_name} Instance",
                # Use processing units
                processing_units=100
            )
            operation = instance.create()
            operation.result()
            print(f"Created instance {instance_id}")

        # Check if database exists
        database = instance.database(database_id)
        try:
            database.reload()
            print(f"Using existing database {database_id}")
            return database
        except Exception:
            print(f"Database {database_id} in instance {instance_id} does not exist")

        # Create new database
        database = instance.database(database_id)
        operation = database.create()
        operation.result()
        print(f"Created database {database_id}")

        return database
    except Exception as e:
        print(f"Error creating dataset: {e}")
        raise


def create_schema_less_graph(database):
    ## read schema-less.sql and execute the DDL statements
    try:
        ddl_statements = []
        with open(os.path.join(os.path.dirname(__file__), 'schema-less.sql'), 'r') as f:
            ddl_statements = f.read().split(';')
            ddl_statements = [stmt.strip() for stmt in ddl_statements if stmt.strip()]

        ## replace SchemaLessGraph with graphName
        ddl_statements = [stmt.replace('SchemaLessGraph', graphName) for stmt in ddl_statements]

        operation = database.update_ddl(ddl_statements)
        operation.result()
        print(f"Created schema-less graph tables")
    except Exception as e:
        print(f"Error creating schema-less graph: {e}")
        raise

def safe_json(x, exclude_columns):
    props = x.drop(exclude_columns)
    # Use allow_nan=False to surface NaN/Inf issues quickly
    json_text = json.dumps(props.to_dict(), ensure_ascii=False, allow_nan=False)
    # json_text empty or none , return '{}'
    if not json_text or json_text == 'null':
        return '{}'
    return json_text
g_allNodeIdsSet = set()
def prepare_data(csv_file, labelName , is_relationship=False):
    """Prepare data by normalizing column names and creating IDs"""
    try:
        labelName = labelName.lower().strip()
        # Read CSV file
        df = pd.read_csv(os.path.join(data_dir, csv_file),  sep=',')
        print(f"Read {len(df)} rows from {csv_file}")
        
        # Convert column names to lowercase
        df.columns = [col.lower().strip() for col in df.columns]

        df["label"] = labelName

        ## convert start with "is" columns to boolean
        is_bool_columns = [col for col in df.columns if col.startswith('is')]
        for col in is_bool_columns:
            df[col] = df[col].astype('bool')


        if "globalstep" in df.columns:
            df['globalstep'] = df['globalstep'].astype('string')
            df['id'] = df['globalstep'].copy()

        # For relationship tables, convert all *Id columns to string
        id_columns = [col for col in df.columns if col.endswith('id')]
        # Handle IDs based on table type
        if is_relationship:
            # first id column is as src_id, second as dest_id
            if len(id_columns) < 2:
                raise ValueError(f"Not enough ID columns found in {csv_file}")
        
            startEndLabelNames = [id_columns[0].replace('_id', ''), id_columns[1].replace('_id', '')]
            
            df["id"] = df[id_columns[0]].astype('string')
            #give id a prefix to avoid id collision between different entity types
            df["id"] = startEndLabelNames[0] + "_" + df["id"]

            df["dest_id"] = df[id_columns[1]].astype('string')
            df["dest_id"] = startEndLabelNames[-1] + "_" + df["dest_id"]

            ## randomly generate id string as primary key, length 24
            df["edge_id"] = [''.join(random.choices(string.ascii_letters + string.digits, k=24)) for _ in range(len(df))]
            df["edge_id"] = df["edge_id"].astype('string')

            # Convert all columns except id, edge_id, dest_id to JSON string and store in properties
            df["properties"] = df.apply(lambda x: safe_json(x, ["id", "label", "edge_id", "dest_id"] + id_columns) , axis=1)

            # Keep only id, edge_id, dest_id, label, properties columns
            df = df[["id", "dest_id", "label", "edge_id", "properties"]]
            # edge_id unique
            df = df.drop_duplicates(subset=['edge_id'])
            # Skip IDs not in g_allNodeIdsSet
            df = df[df["id"].isin(g_allNodeIdsSet)]

        else:
            ## first id column is the primary key
            if len(id_columns) < 1:
                raise ValueError(f"No ID column found in {csv_file}")

            df["id"] = df[id_columns[0]].astype('string')
            ## Give id a prefix with label to avoid id collision between different entity types
            df["id"] = labelName + "_" + df["id"]
            df["properties"] = df.apply(lambda x: safe_json(x, ["id", "label"] + id_columns) , axis=1)
            # Keep only id, label, properties columns
            df = df[["id", "label", "properties"]]
            # id unique 
            df = df.drop_duplicates(subset=['id'])

            df = df[df["id"].isin(g_allNodeIdsSet) == False]
            g_allNodeIdsSet.update(df["id"].values)

        print(f"Prepared data columns: {', '.join(df.columns)}")
        return df
        
    except Exception as e:
        print(f"Error preparing data from {csv_file}: {e}")
        raise e
    


def load_csv_to_spanner(database, df, labelName, is_relationship=False):
    """Load a DataFrame into a Spanner table"""
    try:
        if df is None:
            raise ValueError("DataFrame is None")
        
        labelName = labelName.lower().strip()

        table_name = "GraphEdge" if is_relationship else "GraphNode"
        
        # Prepare data for insertion
        # Spanner requires data in list format
        columns = list(df.columns)
        data = []
        for _, row in df.iterrows():
            row_data = []
            for col in columns:
                val = row[col]
                row_data.append(val)
            data.append(row_data)
        
        # Insert data in batches to avoid exceeding Spanner limit
        # Calculate number of changes per record (number of columns)
        mutations_per_row = len(columns)
        # Spanner limit each transaction to at most 80000 changes
        max_mutations = 80000
        # Calculate maximum batch size
        batch_size = max(1, max_mutations // mutations_per_row)
        
        total_rows = len(data)
        for i in range(0, total_rows, batch_size):
            end_idx = min(i + batch_size, total_rows)
            batch_data = data[i:end_idx]
            
            print(f"Inserting batch {i//batch_size + 1}/{(total_rows + batch_size - 1)//batch_size} " 
                  f"({i} to {end_idx-1} of {total_rows} rows)")

            with database.batch() as batch:
                batch.insert_or_update(
                    table=table_name,
                    columns=columns,
                    values=batch_data
                )
        
        print(f"Loaded {len(df)} rows into {table_name}")
        print(f"Table schema for {table_name}:")
        for col in df.columns:
            print(f"  {col}: {df[col].dtype}")
            
    except Exception as e:
        print(f"Error loading data to {table_name}: {e}")
        raise e

def import_data(database):
        # Define files to load
    ## schema-less all name to lower case
    files_to_load = [
        # # Entity tables
        ("transactions_cleaned.csv", "transaction", False),
        ("merchants.csv", "merchant", False),
        ("clients.csv", "client", False),
        ("banks.csv", "bank", False),
        ("emails.csv", "email" , False),
        ("phonenumbers.csv", "phonenumber", False),
        ("ssns.csv", "ssn", False),
        
        # Relationship tables
        ("Client_Perform_Transaction.csv", "performs", True),
        ("Transaction_To_Client.csv", "to_client" , True),
        ("Transaction_To_Merchant.csv", "to_merchant" , True),
        ("Transaction_To_Bank.csv", "to_bank" , True),
        ("Has_Email.csv", "has_email" , True),
        ("Has_Phonenumber.csv", "has_phone", True),
        ("Has_SSN.csv", "has_ssn", True)
    ]

    # Process and load all files
    for csv_file, table_name, is_relationship in files_to_load:
        print(f"\nProcessing {csv_file} -> {table_name} (is_relationship={is_relationship})")
        df = prepare_data(os.path.join(data_dir, csv_file), table_name, is_relationship)
        if df is not None:
            load_csv_to_spanner(database, df, table_name, is_relationship)

def main():
    try:
        print(f"""Starting import {databaseName} data to Spanner...""")
        client = get_spanner_client()
  
        print("1. Setting up Spanner instance and database...")
        database = create_database(client, instanceName, databaseName)

        print("2. Deleting all existing tables and views...")
        database = delete_all_tables(database)

        print("3. Creating schema-less graph structure...")
        create_schema_less_graph(database)

        print("4. Importing CSV data into Spanner...")

        import_data(database)
        
        print("\nAll data successfully imported to Spanner!")

    except Exception as e:
        print(f"Error importing {databaseName} data to Spanner: {e}")

if __name__ == "__main__":
    main()                  