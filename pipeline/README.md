# Pipeline

This folder contains the Python equivalent of the Redwood-pipeline TypeScript project.

## Quick Start

1. Install pipeline dependencies:
```bash
cd pipeline
pip install -r requirements.txt
```

2. The pipeline uses the same database configuration as your dashboard from `.streamlit/secrets.toml`:
```toml
[DB_CONFIG]
host = "your_host"
port = "your_port"
database = "your_database"
user = "your_user"
password = "your_password"

# Helius API key for token metadata
HELIUS_API_KEY = "your_helius_api_key"
```

The pipeline uses Helius API for token metadata and enhanced transaction data.

3. Run the pipeline:
```bash
python run_pipeline.py
```

## Individual Scripts

You can also run individual scripts:

```bash
# Cleaning
python cleaning/clean_bts_coin_info.py
python cleaning/clean_arb_opportunity.py

# Processing
python processing/process_bts.py
python processing/process_arb.py
```

## Features

- All data goes into a `processed` schema
- Lowercase table names
- Duplicate prevention with row hashing
- Incremental processing
- Independent execution of each process

See `README_PIPELINE.md` for detailed documentation.
