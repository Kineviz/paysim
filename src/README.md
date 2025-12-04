
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
uv run src/data_pipeline.py
```

##  Prepare Data Step-by-Step  (Optional)

### 1. Prepare Transaction Data
```bash
uv run src/prepare_transactions.py
```
Prepares transaction data:
- Converts column names to lowercase
- Generates sequential timestamps based on globalstep
- Output: `data/transactions_cleaned.csv`

### 2. Extract Bank Data
```bash
uv run src/gen_banks.py
```
Extracts unique banks from transactions_cleaned.csv into:
- `data/banks.csv`: Bank entities with IDs and names

### 3. Generate PII Data
```bash
uv run src/gen_pii.py
```
Extracts PII (Personal Identifiable Information) from clients.csv into:
- Entity Tables:
  - `data/emails.csv`: Unique email addresses
  - `data/phonenumbers.csv`: Unique phone numbers
  - `data/ssns.csv`: Unique SSNs
- Relationship Tables:
  - `data/Has_Email.csv`: Links clients to emails
  - `data/Has_Phonenumber.csv`: Links clients to phone numbers
  - `data/Has_SSN.csv`: Links clients to SSNs

### 4. Generate Transaction Relationships
```bash
uv run src/gen_relationships.py
```
Creates relationship tables:
- `data/Client_Perform_Transaction.csv`: Client -> Transaction
- `data/Transaction_To_Client.csv`: Transaction -> Client
- `data/Transaction_To_Merchant.csv`: Transaction -> Merchant
- `data/Transaction_To_Bank.csv`: Transaction -> Bank
