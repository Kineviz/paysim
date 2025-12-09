
#  Injection data into Spanner Emulator

>Tips: Graph Query not supported on Spanner Emulator

> Only need keep project_id and emulator_host fields for Spanner Emulator.
> Currently the spanner emulator do not support TO_JSON or SAFE_TO_JSON functions in queries. So can not return the graph paths in JSON format.


## Start Spanner Emulator
Follow the instructions [here](https://cloud.google.com/spanner/docs/emulator) to start the Spanner Emulator.

```
docker run -p 9010:9010 -p 9020:9020 gcr.io/cloud-spanner-emulator/emulator
```

## Prepare data
> Tips: You can skip this section if you already have the prepared CSV files from the data pipeline.

```bash
# Clone this repository
git clone git@github.com:Kineviz/paysim.git

cd paysim

# Install and setup uv
pip install uv
uv venv --python=python3.11

.venv\Scripts\activate  # Windows; Linux/Mac: source .venv/bin/activate

# Install dependencies
uv pip install pandas google-cloud-bigquery google-cloud-spanner pandas-gbq db-dtypes python-dotenv

# Prepare data to be loaded to Spanner or BigQuery
uv run src/prepare_data.py
```

(If your environment uses `uv` as a runner — keep using it. Otherwise run the
pipeline with the command your environment expects.)

## Run the import

Add "emulator_host:localhost:9010" to your `./data-injection/spanner/google_auth_keyfile.json` file for Spanner Emulator connection.

Then run the import script:

```bash
uv run data-injection/spanner/import_paysim.py
```


## Test queries

You can run the included test queries to validate the import:

```bash
uv run data-injection/spanner/test_queries.py
```