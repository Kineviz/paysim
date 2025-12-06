# Paysim → Google Cloud Spanner (property graph schema)

Back to [Home](../../README.md)

This folder contains a small import tool that loads Paysim transaction data into
Google Cloud Spanner using property graph schema.

## Prerequisites

- A Google Cloud project with Spanner instance and database created.
- A service account JSON key with permissions to use Spanner.
- Python 3.8+ (or your project's required runtime) and project dependencies.
- The Paysim data prepared by the repository data pipeline.

## Prepare data
> You can skip this section if you already have the prepared CSV files from the data pipeline.

Run the local data pipeline to generate the datasets used by the importer:

```powershell
uv run src/prepare_data.py
```

(If your environment uses `uv` as a runner — keep using it. Otherwise run the
pipeline with the command your environment expects.)

## Configure Google service account json keyfile information

1. Copy the example keyfile and add your service account credentials:

```powershell
cd data-injection/spanner-schema
cp google_auth_keyfile.example.json google_auth_keyfile.json
```

2. Edit `google_auth_keyfile.json` and paste the service-account key JSON you
   downloaded from the GCP Console.

## Configure import settings

Open `import_paysim.py` and set the target Spanner details:

```python
instanceName = "your-instance-name"
databaseName = "your-database-name"
graphName = "your-graph-name"
```

Adjust any other settings in `import_paysim.py` if needed (batch sizes, CSV
paths, etc.).

## Run the import

From this folder run:

```powershell
uv run data-injection/spanner-schema/import_paysim.py
```

The script will read the prepared CSVs and load nodes/edges into the Spanner
as tables, then run DDL declared in graph_view.sql to create a graph.

## Test queries

You can run the included test queries to validate the import:

```powershell
uv run data-injection/spanner-schema/test_queries.py
```

Example (graph query to fetch a Client → Transaction path):

```sql
GRAPH graph_view
MATCH (c:Client)-[p:PERFORMS]->(t:Transaction)
RETURN TO_JSON([TO_JSON(c), TO_JSON(p), TO_JSON(t)]) AS path_json
LIMIT 1
```

The test checks:
- node connectivity (Client → Transaction)
- edge properties on `PERFORMS`
- JSON formatting and basic type consistency

## Troubleshooting

- If authentication fails, ensure `google_auth_keyfile.json` is valid and the
  service account has Spanner Database Admin access, for table creation and uploading.
- **Important:** Spanner Graph is only available on Spanner Enterprise, not on Spanner Standard.

### GCP Console Setup Steps

If you haven't set up your GCP resources yet, follow these steps:

1. Create a GCP project
2. Create a Spanner instance (select **Enterprise** edition for graph support)
3. Create a database within the instance
4. Create a service account with the **Spanner Database Admin** role
5. Download the service account JSON key file
