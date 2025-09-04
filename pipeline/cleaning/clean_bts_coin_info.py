#!/usr/bin/env python3
"""
Clean BTS Coin Info Data
Python equivalent of cleanBTSCoinInfo.ts
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from ..pipeline_config import db_config, logger, BATCH_SIZE, SCHEMA_NAME
from ..pipeline_utils import (
    setup_processed_schema, 
    get_last_processed_id, 
    update_pipeline_tracker,
    generate_row_hash,
    create_table_if_not_exists,
    batch_insert,
    fetch_data_with_pagination
)

def create_bts_coin_info_table():
    """Create the cleaned BTS coin info table"""
    columns_definition = """
        id BIGINT PRIMARY KEY,
        tokenAddress VARCHAR(255),
        symbol VARCHAR(100),
        name VARCHAR(255),
        totalSupply DECIMAL,
        circulatingSupply DECIMAL,
        marketCap DECIMAL,
        price DECIMAL,
        volume24h DECIMAL,
        liquidityRatio DECIMAL,
        devHoldings DECIMAL,
        devHoldingsPercentage DECIMAL,
        contractAddress VARCHAR(255),
        network VARCHAR(100),
        decimals INTEGER,
        website VARCHAR(500),
        twitter VARCHAR(500),
        telegram VARCHAR(500),
        description TEXT,
        logoUrl VARCHAR(500),
        createdAt TIMESTAMP,
        updatedAt TIMESTAMP,
        row_hash VARCHAR(64) UNIQUE
    """
    
    create_table_if_not_exists('clean_bts_coin_info', columns_definition)

def clean_bts_coin_info():
    """Clean and copy BTS coin info data"""
    try:
        logger.info("🔄 Starting BTS Coin Info cleaning process...")
        
        # Setup schema and table
        setup_processed_schema()
        create_bts_coin_info_table()
        
        # Get last processed ID
        last_id = get_last_processed_id('clean_bts_coin_info')
        logger.info(f"📌 Starting from ID: {last_id}")
        
        # Fetch data from raw table
        raw_data = fetch_data_with_pagination('btscoininfo', last_id, BATCH_SIZE)
        
        if not raw_data:
            logger.info("⚠️ No new data to process")
            return
        
        logger.info(f"📊 Processing {len(raw_data)} rows...")
        
        # Process data in batches
        processed_data = []
        for row in raw_data:
            # Create processed row (excluding row_hash and datecaptured)
            processed_row = {k: v for k, v in row.items() 
                           if k.lower() != 'row_hash' and k.lower() != 'datecaptured'}
            
            # Generate new row hash
            processed_row['row_hash'] = generate_row_hash(processed_row)
            processed_data.append(processed_row)
        
        # Insert processed data
        columns = list(processed_data[0].keys())
        batch_insert('clean_bts_coin_info', processed_data, columns)
        
        # Update tracker
        max_id = max(row['id'] for row in raw_data)
        update_pipeline_tracker('clean_bts_coin_info', max_id)
        
        logger.info(f"✅ BTS Coin Info cleaning completed. Processed up to ID: {max_id}")
        
    except Exception as e:
        logger.error(f"❌ Error in BTS Coin Info cleaning: {e}")
        raise

if __name__ == "__main__":
    clean_bts_coin_info()
