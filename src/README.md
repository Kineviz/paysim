
# PaySim Data Preparation Pipeline 

Back to [Home](../README.md)

## Quick Start 

```bash
# Install uv
pip install uv

# Setup environment
uv venv --python=python3.11
.venv\Scripts\activate  # Windows; Linux/Mac: source .venv/bin/activate
uv pip install pandas google-cloud-bigquery google-cloud-spanner db-dtypes

# Run full pipeline
uv run src/prepare_data.py
```

##  Prepare Data Step-by-Step  (Optional)

### 1. Prepare Transaction Data
```bash
uv run src/prepare_transactions.py
```
Prepares transaction data:
- Converts column names to lowercase
- Generates sequential timestamps based on globalstep
- Output: `data/processed/transactions_cleaned.csv`

### 2. Extract Bank Data
```bash
uv run src/gen_banks.py
```
Extracts unique banks from `data/raw/transactions.csv` into:
- `data/processed/banks.csv`: Bank entities with IDs and names

### 3. Generate PII Data
```bash
uv run src/gen_pii.py
```
Extracts PII (Personal Identifiable Information) from `data/raw/clients.csv` into:
- Entity Tables:
  - `data/processed/emails.csv`: Unique email addresses
  - `data/processed/phonenumbers.csv`: Unique phone numbers
  - `data/processed/ssns.csv`: Unique SSNs
- Relationship Tables:
  - `data/processed/Has_Email.csv`: Links clients to emails
  - `data/processed/Has_Phonenumber.csv`: Links clients to phone numbers
  - `data/processed/Has_SSN.csv`: Links clients to SSNs

### 4. Generate Transaction Relationships
```bash
uv run src/gen_relationships.py
```
Creates relationship tables from `data/processed/transactions_cleaned.csv`:
- `data/processed/Client_Perform_Transaction.csv`: Client -> Transaction
- `data/processed/Transaction_To_Client.csv`: Transaction -> Client
- `data/processed/Transaction_To_Merchant.csv`: Transaction -> Merchant
- `data/processed/Transaction_To_Bank.csv`: Transaction -> Bank

## Data Organization

- **`data/raw/`**: Original PaySim CSV files (input)
  - `transactions.csv`
  - `clients.csv`
  - `merchants.csv`

- **`data/processed/`**: Generated CSV files (output from pipeline)
  - All entity and relationship tables created by the pipeline
