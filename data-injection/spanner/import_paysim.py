# Import required libraries
import sys
import pandas as pd 
from google.cloud import spanner
from google.oauth2 import service_account
import os
import json

# Load configuration from config.json
def load_config():
    """Load configuration from config.json file"""
    config_path = os.path.join(os.path.dirname(__file__), 'config.json')
    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Configuration file not found: {config_path}\n"
            "Please copy config.example.json to config.json and update it with your settings."
        )
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Validate required fields
    required_fields = ['instance_name', 'database_name', 'graph_name', 'google_auth_keyfile']
    for field in required_fields:
        if field not in config:
            raise ValueError(f"Missing required configuration field: {field}")
    
    return config

# Load configuration
config = load_config()
instanceName = config['instance_name']
databaseName = config['database_name']
graphName = config['graph_name']
google_auth_keyfile = config['google_auth_keyfile']

data_dir = os.path.join(os.path.dirname(__file__), './../../', 'data')
raw_data_dir = os.path.join(data_dir, 'raw')
processed_data_dir = os.path.join(data_dir, 'processed')

def delete_all_tables(database):
    """Delete all tables in the specified database"""
    try:
        # try to delete graph first;
        operation=database.update_ddl([f'''DROP PROPERTY GRAPH IF EXISTS `{graphName}`''']);
        operation.result()
        print(f"Deleting graph at first")


        # try to delete all views first
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(
                "SELECT table_name FROM information_schema.views WHERE table_schema = ''"
            )
            views = [row[0] for row in results]
        
        if views:
            print(f"Deleting views: {', '.join(views)}")
            # Build DDL statements to drop views
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
            
        # Build DDL statements to drop tables
        ddl_statements = [f"DROP TABLE {table}" for table in tables]
        
        # Execute DDL statements to drop tables
        operation = database.update_ddl(ddl_statements)
        operation.result()
        
        print(f"Deleted tables: {', '.join(tables)}")
        return database
    except Exception as e:
        print(f"Error deleting tables: {e}")
        return database



def create_dataset(spanner_client, instance_name, database_name):
    """Create a new instance and database in Spanner"""
    try:
        
        # Get or create instance
        instance_id = f"{instance_name}"
        
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
        
        
        database_id = database_name
        
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
        
        # Convert boolean fields if present
        if 'isfraud' in df.columns:
            df['isfraud'] = df['isfraud'].astype('bool')
        if 'isflaggedfraud' in df.columns:
            df['isflaggedfraud'] = df['isflaggedfraud'].astype('bool')
        if 'highrisk' in df.columns:
            df['highrisk'] = df['highrisk'].astype('bool')
        
        print(f"Prepared data columns: {', '.join(df.columns)}")
        if not is_relationship:
            print(f"ID column type: {df['id'].dtype}")
        else:
            print(f"ID column types: {', '.join(f'{col}: {df[col].dtype}' for col in id_columns)}")
        return df
        
    except Exception as e:
        print(f"Error preparing data from {csv_file}: {e}")
        return None

def load_csv_to_spanner(database, df, table_name):
    """Load a DataFrame into a Spanner table"""
    try:
        if df is None:
            raise ValueError("DataFrame is None")
            
        # Check if this is a relationship table
        is_relationship = any(table_name.startswith(prefix) for prefix in 
                            ['Has_', 'Client_Perform_', 'Transaction_To_'])
        
        # Check if table exists, delete if it does
        try:
            with database.snapshot() as snapshot:
                # Use INFORMATION_SCHEMA.TABLES to check if table exists
                results = snapshot.execute_sql(
                    f"SELECT table_name FROM INFORMATION_SCHEMA.TABLES WHERE table_name = '{table_name}'"
                )
                # Convert results to list, check if table exists
                table_exists = len(list(results)) > 0
                
            if table_exists:
                print(f"Table {table_name} exists, deleting...")
                ddl_statement = f"DROP TABLE {table_name}"
                operation = database.update_ddl([ddl_statement])
                operation.result()
                print(f"Table {table_name} deleted")
        except Exception as e:
            print(f"Error checking or deleting table {table_name}: {e}")
        
        # Create column definitions
        column_defs = []
        primary_key = ""
        
        # Create column mapping, record data type for each field
        column_types = {}
        
        if is_relationship:
            # For relationship tables, set all ID columns to STRING
            for col in df.columns:
                if col.endswith('_id'):
                    column_defs.append(f"{col} STRING(36) NOT NULL")
                    column_types[col] = "STRING"
                elif col == "timestamp":
                    column_defs.append(f"{col} STRING(30)")
                    column_types[col] = "STRING"
                else:
                    column_defs.append(f"{col} FLOAT64")
                    column_types[col] = "FLOAT64"
            
            # For relationship tables, we can use the combination of ID columns as the primary key
            id_columns = [col for col in df.columns if col.endswith('_id')]
            if id_columns:
                primary_key = f") PRIMARY KEY ({', '.join(id_columns)}"
            
        else:
            # For entity tables, set id as primary key
            column_defs.append("id STRING(36) NOT NULL")
            column_types["id"] = "STRING"
            
            # Add other columns
            for col in df.columns:
                if col != 'id':
                    if col in ['isfraud', 'isflaggedfraud', 'highrisk']:
                        # Boolean type field defined as BOOL, default to false
                        column_defs.append(f"{col} BOOL NOT NULL DEFAULT (false)")
                        column_types[col] = "BOOL"
                    elif df[col].dtype == 'float64' or df[col].dtype == 'int64':
                        column_defs.append(f"{col} FLOAT64")
                        column_types[col] = "FLOAT64"
                    else:
                        column_defs.append(f"{col} STRING(255)")
                        column_types[col] = "STRING"
            
            # Add primary key constraint
            primary_key = ") PRIMARY KEY (id"
        
        # Create DDL statement
        columns_str = ", ".join(column_defs)
        create_table_ddl = f"CREATE TABLE {table_name} ({columns_str}{primary_key})"
        
        # Use database object directly to execute DDL
        operation = database.update_ddl([create_table_ddl])
        operation.result()
        print(f"Created table {table_name}")
        
        # Ensure data type matches table definition
        for col in df.columns:
            if column_types.get(col) == "STRING":
                df[col] = df[col].astype('string')
            elif column_types.get(col) == "FLOAT64":
                try:
                    df[col] = df[col].astype('float64')
                except:
                    pass
            elif column_types.get(col) == "BOOL":
                try:
                    df[col] = df[col].astype('bool')
                except:
                    # If conversion fails, use 0 and non-0 values
                    df[col] = df[col].astype('int64').astype('bool')
        
        # Prepare data for insertion
        # Spanner requires data in list format
        columns = list(df.columns)
        data = []
        for _, row in df.iterrows():
            row_data = []
            for col in columns:
                val = row[col]
                # Ensure value matches column type
                if column_types.get(col) == "STRING" and not isinstance(val, str):
                    val = str(val)
                # Boolean values remain unchanged, Spanner API will handle correctly
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
                batch.insert(
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
        raise


def create_graph(database):
    """Create property graph view in Spanner database"""
    try:

        operation=database.update_ddl([f'''DROP PROPERTY GRAPH IF EXISTS `{graphName}`'''])
        operation.result()
        print(f"Deleting property graph at first")
    
        sql_path =  os.path.join(os.path.dirname(__file__), 'graph_view.sql')
        if not os.path.exists(sql_path):
            raise FileNotFoundError(f"No file found: {sql_path}")
        
        sql = ''    
        with open(sql_path, 'r', encoding='utf-8', newline='\n') as file:
            sql = file.read()

        ## replace GRAPH graph_view name to the variable
        sql = sql.replace('graph_view', f'{graphName}')

        ## remove last ;
        if sql.strip().endswith(';'):
            sql = sql.strip()[:-1]
        
        # Execute SQL to create graph view
        print(f"Creating Property Graph({graphName})...")
        operation=database.update_ddl([sql])
        operation.result()
        print(f"Property graph({graphName}) created successfully")
        
    except Exception as e:
        print(f"Error creating Property Graph({graphName}): {e}", file=sys.stderr)
        raise

def main():
    try:
        print("Starting import PaySim data to Spanner...")
        
        # Initialize Spanner client
        print("1. Initializing Spanner client...")
        auth_keyfile_path = os.path.join(os.path.dirname(__file__), google_auth_keyfile)
        if not os.path.exists(auth_keyfile_path):
            raise FileNotFoundError(
                f"Service account key file not found: {auth_keyfile_path}\n"
                "Please ensure the google_auth_keyfile path in config.json is correct."
            )
        credentials = service_account.Credentials.from_service_account_file(auth_keyfile_path)
        client = spanner.Client(credentials=credentials)
        print(f"Connected to GCP project: {client.project}")

        # Create Spanner instance and database
        print("2. Setting up Spanner instance and database...")
        try:
            database = create_dataset(client, instanceName, databaseName)
            print("Spanner instance and database ready")
        except Exception as e:
            print(f"Error setting up Spanner instance and database: {e}")
            raise

        # First delete all existing tables
        print("3. Deleting all existing tables and views...")
        try:
            database = delete_all_tables(database)
            print("Database cleanup completed")
        except Exception as e:
            print(f"Error cleaning database: {e}")
            print("Continuing with data import...")

        # Define files to load
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

        print("4. Importing data files into Spanner...")

        # Process and load all files
        for csv_file, table_name, is_transaction in files_to_load:
            print(f"\nProcessing {csv_file} -> {table_name}")
            # Prepare data
            df = prepare_data(csv_file, is_transaction)
            if df is not None:
                # Load to Spanner
                load_csv_to_spanner(database, df, table_name)
                
        print("\n All data successfully imported to Spanner!")

        print("5. Creating Property Graph view...")
        
        # # Create property graph 
        create_graph(database)

        print("\n PaySim data import and graph creation completed successfully!")

    except Exception as e:
        print(f"Error importing PaySim data to Spanner: {e}")

if __name__ == "__main__":
    main()