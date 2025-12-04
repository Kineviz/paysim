# Import required libraries
import sys
import pandas as pd 
import os
from google.cloud import bigquery
from google.oauth2 import service_account

datasetName="paysim_graph"
graphName="graph_view"

data_dir = os.path.join(os.path.dirname(__file__), './../../', 'data')

def run_test_query(client):
    """Execute the property creation SQL using BigQuery client"""
    try:
        query = f"""
    GRAPH {datasetName}.{graphName}
    MATCH
    (c:Client)-[p:PERFORMS]->(t:Transaction)
    RETURN 
    TO_JSON(
        [TO_JSON(c), TO_JSON(p), TO_JSON(t)]
    ) as path_json
    LIMIT 1
        """
        
        # Run the query
        job = client.query(query)
        job.result()  # Wait for query to complete
        for row in job:
            print(row["path_json"])
        print("Successfully ran test query")
    except Exception as e:
        print(f"Error running test query: {e}", file=sys.stderr)
        raise e



def main():
    # Initialize BigQuery client
    credentials = service_account.Credentials.from_service_account_file(
        os.path.join(os.path.dirname(__file__), 'google_auth_keyfile.json')
    )
    client = bigquery.Client(credentials=credentials, project=credentials.project_id)

    run_test_query(client)

if __name__ == "__main__":
    main()