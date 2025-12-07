import os
import sys
from pathlib import Path
from google.cloud import spanner
from google.oauth2 import service_account


instanceName = "demo-2025"
databaseName = "paysim_schemaless"
graphName = "paysim_schemaless_graph"


def run_test_query(client, database):
    """Run a test query on the Spanner database"""
    
    
    query = f"""
GRAPH {graphName}
MATCH (n:client)-[r:performs]->(m:transaction)
RETURN SAFE_TO_JSON(
    [TO_JSON(n), TO_JSON(r), TO_JSON(m)]
  ) as path_json
LIMIT 1
    """
    
    try:
       
        with database.snapshot() as snapshot:
            results = snapshot.execute_sql(query)
            
            print("\nTest Query Results:")
            for row in results:
                print(row[0])
            
        return True
        
    except Exception as e:
        print(f"\nError executing query: {e}", file=sys.stderr)
        return False

def main():
    try:
        # Initialize Spanner client
        credentials = service_account.Credentials.from_service_account_file(
            os.path.join(os.path.dirname(__file__), 'google_auth_keyfile.json')
        )
 
        client = spanner.Client(credentials=credentials)
        
        instance = client.instance(instanceName)
        database = instance.database(databaseName)
        
        success = run_test_query(client, database)
        
        sys.exit(0 if success else 1)
        
    except Exception as e:
        print(f"错误: {e}", file=sys.stderr)
        sys.exit(1)

if __name__ == "__main__":
    main() 