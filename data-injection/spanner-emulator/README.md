###  Injection data into Spanner Emulator

>Tips: Graph Query not supported on Spanner Emulator

Add "emulator_host:localhost:9010" to your `./../spanner/google_auth_keyfile.json` file used for authentication.
> Only need keep project_id and emulator_host fields for Spanner Emulator.
> Currently the spanner emulator do not support TO_JSON or SAFE_TO_JSON functions in queries. So can not return the graph paths in JSON format.

## Start Spanner Emulator
Follow the instructions [here](https://cloud.google.com/spanner/docs/emulator) to start the Spanner Emulator.

```
cd data-injection/spanner-emulator
docker-compose up -d
```


## Run the import

From this folder run:

```powershell
uv run data-injection/spanner/import_paysim.py
```


## Test queries

You can run the included test queries to validate the import:

```powershell
uv run data-injection/spanner/test_queries.py
```