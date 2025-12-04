# PaySim Graph Data Processing

Transform PaySim transaction data into property graph format for fraud detection analysis.

## Quick Start

```bash
# Install and setup
pip install uv
uv venv --python=python3.11
.venv\Scripts\activate  # Windows; Linux/Mac: source .venv/bin/activate
uv pip install pandas google-cloud-bigquery google-cloud-spanner pandas-gbq db-dtypes

# Generate graph data
uv run src/data_pipeline.py
```

**Requirements:** Python 3.11, GCP credentials, PaySim CSVs (`transactions.csv`, `clients.csv`, `merchants.csv`)

## Data Pipeline

See [Data Preparation Pipeline](src/README.md) for details. The pipeline generates:

- **Entity nodes:** Banks, Emails, PhoneNumbers, SSNs
- **PII relationships:** Client → Email/Phone/SSN
- **Transaction relationships:** Client ↔ Transaction ↔ Merchant/Bank

## Import to Cloud

- [Spanner with Schema](data-injection/spanner-schema/README.md)
- [Spanner Schema-less](data-injection/spanner-schema-less/README.md) *(Note: uses lowercase labels)*
- [BigQuery](data-injection/biguqery/README.md)

## Graph Schema

```mermaid
graph LR
    Client["<b>Client</b><br/>id, name, isfraud"]
    Transaction["<b>Transaction</b><br/>id, amount, timestamp<br/>action, globalstep<br/>isfraud, isflaggedfraud<br/>typedest, typeorig"]
    Merchant["<b>Merchant</b><br/>id, name, highrisk"]
    Bank["<b>Bank</b><br/>id, name"]
    Email["<b>Email</b><br/>id, name"]
    PhoneNumber["<b>PhoneNumber</b><br/>id, name"]
    SSN["<b>SSN</b><br/>id, name"]
    
    Client -->|PERFORMS| Transaction
    Transaction -->|TO_CLIENT| Client
    Transaction -->|TO_MERCHANT| Merchant
    Transaction -->|TO_BANK| Bank
    Client -->|HAS_EMAIL| Email
    Client -->|HAS_PHONE| PhoneNumber
    Client -->|HAS_SSN| SSN
    
    style Client fill:#e1f5ff
    style Transaction fill:#fff3e0
    style Merchant fill:#f3e5f5
    style Bank fill:#e8f5e9
    style Email fill:#fce4ec
    style PhoneNumber fill:#fce4ec
    style SSN fill:#fce4ec
```

**Nodes:** Client, Transaction, Merchant, Bank, Email, PhoneNumber, SSN  
**Edges:** PERFORMS, TO_CLIENT, TO_MERCHANT, TO_BANK, HAS_EMAIL, HAS_PHONE, HAS_SSN
