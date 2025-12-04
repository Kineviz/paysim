# Paysim → Google Cloud BigQuery (property graph schema)

Back to [Home](../../README.md)

This folder contains a small import tool that loads Paysim transaction data into
Google Cloud BigQuery using property graph schema.


## Prepare data
> You can skip this section if you already have the prepared CSV files from the data pipeline.

Run the local data pipeline to generate the datasets used by the importer:

```powershell
uv run src/data_pipeline.py
```

(If your environment uses `uv` as a runner — keep using it. Otherwise run the
pipeline with the command your environment expects.)

## Configure Google service account json keyfile information

1. Copy the example keyfile and add your service account credentials:

```powershell
cd data-injection/biguqery
cp google_auth_keyfile.example.json google_auth_keyfile.json
```

2. Edit `google_auth_keyfile.json` and paste the service-account key JSON you
   downloaded from the GCP Console.

## Configure import settings

Open `import_to_bigquery.py` and set the target BigQuery details:

```python
dataset = "your-dataset-name"
graphName="your-property-graph-name"
```

Adjust any other settings in `import_to_bigquery.py` if needed (batch sizes, CSV
paths, etc.).

## Run the import

From this folder run:

```powershell
uv run  data-injection/biguqery/import_to_bigquery.py
```

The script will read the prepared CSVs and load nodes/edges into the BigQuery
graph.

## Test queries

You can run the included test queries to validate the import:

```powershell
uv run  data-injection/biguqery/test_queries.py
```

Example (graph query to fetch a Client → Transaction path):

```sql
GRAPH graph_view
MATCH (c:Client)-[p:PERFORMS]->(t:Transaction)
RETURN TO_JSON([TO_JSON(c), TO_JSON(p), TO_JSON(t)]) AS path_json
LIMIT 1
```


## Troubleshooting

- If authentication fails, ensure `google_auth_keyfile.json` is valid and the
  service account has BigQuery access.

