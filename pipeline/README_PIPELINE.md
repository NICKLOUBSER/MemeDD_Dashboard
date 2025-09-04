# Python Pipeline for MemeDD Dashboard

This is the Python equivalent of the Redwood-pipeline TypeScript project, designed to process and clean trading data for the MemeDD Dashboard.

## Features

- **Modular Design**: Each process can be run independently
- **Duplicate Prevention**: Uses row hashing to prevent duplicate data insertion
- **Incremental Processing**: Tracks last processed ID to avoid reprocessing
- **Schema Isolation**: All processed data goes into a 'processed' schema
- **Lowercase Tables**: All table names are lowercase for consistency
- **Async Support**: Supports both sync and async processing functions

## Project Structure

```
├── pipeline_config.py          # Database configuration and constants
├── pipeline_utils.py           # Common utility functions
├── pipeline_runner.py          # Main runner script
├── cleaning/                   # Data cleaning scripts
│   └── clean_bts_coin_info.py
├── processing/                 # Data processing scripts
│   ├── process_bts.py
│   └── process_arb.py
├── requirements.txt            # Python dependencies
└── README_PIPELINE.md         # This file
```

## Setup

### 1. Install Dependencies

```bash
pip install -r requirements.txt
```

### 2. Database Configuration

The pipeline uses the same database configuration as your Streamlit dashboard from `.streamlit/secrets.toml`:

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

### 3. Database Setup

The pipeline will automatically create:
- A `processed` schema in your destination database
- A `pipeline_tracker` table to track processing progress
- All necessary tables for processed data

## Usage

### List Available Processes

```bash
python pipeline_runner.py list
```

### Run a Single Process

```bash
# Clean BTS coin info
python pipeline_runner.py clean_bts_coin_info

# Process BTS transactions
python pipeline_runner.py process_bts

# Process arbitrage transactions
python pipeline_runner.py process_arb
```

### Run All Processes

```bash
python pipeline_runner.py all
```

### Run Individual Scripts Directly

You can also run each script directly:

```bash
# Cleaning scripts
python cleaning/clean_bts_coin_info.py

# Processing scripts
python processing/process_bts.py
python processing/process_arb.py
```

## Available Processes

### Cleaning Processes

- **clean_bts_coin_info**: Cleans and copies BTS coin info data from raw to processed schema
  - Removes `row_hash` and `datecaptured` fields
  - Generates new row hashes for duplicate prevention
  - Tracks processing progress

### Processing Processes

- **process_bts**: Processes BTS (sniper bot) transaction data
  - Pairs buy/sell transactions by token address
  - Calculates profits and win/loss outcomes
  - Fetches coin metadata from API
  - Handles partial sells
  - Generates comprehensive trade records

- **process_arb**: Processes arbitrage transaction data
  - Calculates profit and profit percentage
  - Determines win/loss outcomes
  - Creates processed arbitrage records

## Database Schema

### Pipeline Tracker

```sql
CREATE TABLE processed.pipeline_tracker (
    table_name VARCHAR(100) PRIMARY KEY,
    last_processed_id BIGINT DEFAULT 0,
    last_processed_ts TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

### Processed Tables

#### clean_bts_coin_info
Contains cleaned BTS coin information with all metadata fields.

#### processed_bts
Contains paired BTS transactions with calculated fields:
- Buy/sell amounts and prices
- Profit calculations
- Win/loss outcomes
- Token metadata (symbol, name)
- Partial sell information

#### processed_arb
Contains processed arbitrage transactions with:
- Exchange information
- Profit calculations
- Win/loss outcomes
- Token metadata

## Key Features

### Duplicate Prevention
- Each table has a `row_hash` field with a unique index
- Uses `ON CONFLICT (row_hash) DO NOTHING` for safe inserts
- Prevents data duplication across runs

### Incremental Processing
- Tracks `last_processed_id` in `pipeline_tracker`
- Only processes new data since last run
- Efficient for large datasets

### Error Handling
- Comprehensive logging to `pipeline.log`
- Transaction rollback on errors
- Graceful handling of API failures

### Performance
- Batch processing with configurable batch sizes
- Async API calls for coin info fetching
- Efficient database operations

## Logging

All operations are logged to:
- Console output
- `pipeline.log` file

Log levels include:
- INFO: Normal operations
- WARNING: Non-critical issues
- ERROR: Critical failures

## Configuration

Key configuration options in `pipeline_config.py`:

- `BATCH_SIZE`: Number of records to process per batch (default: 5000)
- `CONCURRENT_LIMIT`: Maximum concurrent API calls (default: 10)
- `SCHEMA_NAME`: Target schema name (default: 'processed')

## Troubleshooting

### Common Issues

1. **Database Connection Errors**
   - Verify DB_CONFIG settings in `.streamlit/secrets.toml`
   - Check database permissions
   - Ensure databases are accessible

2. **Missing Dependencies**
   - Run `pip install -r requirements.txt`
   - Check Python version compatibility

3. **API Errors**
   - The pipeline uses Helius API
   - Verify HELIUS_API_KEY is configured in secrets
   - Check API rate limits
   - Ensure network connectivity

4. **Duplicate Data**
   - Check `row_hash` generation logic
   - Verify unique constraints
   - Review data source for duplicates

### Debug Mode

For detailed debugging, modify logging level in `pipeline_config.py`:

```python
logging.basicConfig(level=logging.DEBUG, ...)
```

## Migration from TypeScript

This Python version maintains compatibility with the original TypeScript pipeline:

- Same table structures (with lowercase names)
- Same processing logic
- Same data flow
- Enhanced error handling and logging
- Better modularity and reusability

## Contributing

When adding new processes:

1. Create the script in appropriate directory (`cleaning/` or `processing/`)
2. Add process to `PROCESSES` dictionary in `pipeline_runner.py`
3. Follow the established patterns for error handling and logging
4. Update this README with new process documentation
