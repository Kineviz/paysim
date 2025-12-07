import json
import os
import sys
from pathlib import Path
from google.cloud import spanner
from google.oauth2 import service_account
from google.auth.credentials import AnonymousCredentials
from dotenv import load_dotenv

#automatically load .env file
load_dotenv()

# Load configuration
instanceName = os.getenv('INSTANCE_NAME')
if not instanceName:
    print("Error: INSTANCE_NAME is not set in environment variables.", file=sys.stderr)
    sys.exit(1)
databaseName = os.getenv('DATABASE_NAME') or "paysim"
graphName = os.getenv('GRAPH_NAME') or "graph_view"
google_auth_keyfile = os.getenv('GOOGLE_AUTH_KEYFILE') or 'google_auth_keyfile.json'


def run_test_query(query, database):
    """Run a test query on the Spanner database"""
    try:
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
            
            print("\nTest Query Results:")
            for row in results:
                print(f"test data: {row}")
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
        
        success = run_test_query(f'''
GRAPH {graphName}
MATCH (n)
RETURN DISTINCT LABELS(n)[0] as lableName, COUNT(n) as cnt
GROUP BY lableName
                                 ''', database)
        
        success = run_test_query(f'''
GRAPH {graphName}
MATCH
  (n:client)-[r]->(m:transaction)
RETURN TO_JSON(n) as n_node, TO_JSON(r) as r_relationship, TO_JSON(m) as m_node
LIMIT 1
''', database)
        
        success = run_test_query(f'''
SELECT count(*) as node_count
FROM GraphNode 
''', database)
        
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 