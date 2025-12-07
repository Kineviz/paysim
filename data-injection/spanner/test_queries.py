import json
import os
import sys
from pathlib import Path
from google.cloud import spanner
from google.oauth2 import service_account
from google.auth.credentials import AnonymousCredentials

instanceName = "demo-2025"
databaseName = "paysim"
graphName = "graph_view"

def run_test_query(client, database):
    """Run a test query on the Spanner database"""
    
    
    query = f"""
GRAPH {graphName}
MATCH
  (n:Client)-[r:PERFORMS]->(m:Transaction)
RETURN TO_JSON(
    [TO_JSON(n), TO_JSON(r), TO_JSON(m)]
  ) as path_json
LIMIT 1
    """
    
    try:
       
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
            
            print("\nTest Query Results:")
            for row in results:
                print(f"Client ID: {row[0]}")
            
        return True
        
    except Exception as e:
        print(f"\nError executing query: {e}", file=sys.stderr)
        return False

def main():
    try:
        # Initialize Spanner client
        auth_keyfile_path =  os.path.join(os.path.dirname(__file__), 'google_auth_keyfile.json')
        authJSON = json.load(open(auth_keyfile_path))
        project_id = authJSON.get("project_id")
        emulator_host = os.getenv("SPANNER_EMULATOR_HOST") or authJSON.get("emulator_host")
        if emulator_host:
            print(f"Connecting to Spanner emulator at {emulator_host} for project: {project_id}")
            client = spanner.Client(project=project_id, 
                                    credentials=AnonymousCredentials(),
                                    client_options={"api_endpoint": emulator_host}
                                    )
            print(f"Connected to Spanner emulator: {client.project}")
        else:
            print(f"Connecting to GCP Spanner project: {project_id}")
            credentials = service_account.Credentials.from_service_account_file(auth_keyfile_path)
            client = spanner.Client(credentials=credentials)
            print(f"Connected to GCP project: {client.project}")
        
        instance = client.instance(instanceName)
        database = instance.database(databaseName)
        
        success = run_test_query(client, database)
        
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 