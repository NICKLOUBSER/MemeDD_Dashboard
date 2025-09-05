#!/usr/bin/env python3
"""
Clean BTS Coin Info Data
Python equivalent of cleanBTSCoinInfo.ts
"""

import sys
import os
import time
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline_config import db_config, logger, BATCH_SIZE, SCHEMA_NAME
from pipeline_utils import (
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
        coinPrice DECIMAL,
        devPubkey VARCHAR(255),
        devCapital DECIMAL,
        devholderPercentage DECIMAL,
        tokenSupply DECIMAL,
        totalHoldersSupply DECIMAL,
        isBundle BOOLEAN,
        liquidityToMcapRatio DECIMAL,
        reservesInSOL DECIMAL,
        dateCaptured TIMESTAMP,
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
        
        total_processed = 0
        batch_count = 0
        
        while True:
            # Fetch data from raw table
            raw_data = fetch_data_with_pagination('btscoininfo', last_id, BATCH_SIZE)
            
            if not raw_data:
                logger.info("⚠️ No more data to process")
                break
            
            batch_count += 1
            logger.info(f"📊 Processing batch {batch_count}: {len(raw_data)} rows...")
            
            # Process data in batches
            processed_data = []
            for row in raw_data:
                # Create processed row (excluding row_hash and datecaptured)
                processed_row = {}
                for k, v in row.items():
                    if k.lower() != 'row_hash' and k.lower() != 'datecaptured':
                        # Handle empty strings for numeric fields
                        if k.lower() in ['coinprice', 'devcapital', 'devholderpercentage', 'tokensupply', 'totalholderssupply', 'liquiditytomcapratio', 'reservesinsol']:
                            processed_row[k.lower()] = v if v and str(v).strip() else None
                        else:
                            processed_row[k.lower()] = v
                
                # Generate new row hash
                processed_row['row_hash'] = generate_row_hash(processed_row)
                processed_data.append(processed_row)
            
            # Insert processed data
            columns = list(processed_data[0].keys())
            batch_insert('clean_bts_coin_info', processed_data, columns)
            
            # Update tracker and last_id for next batch
            max_id = max(row['id'] for row in raw_data)
            update_pipeline_tracker('clean_bts_coin_info', max_id)
            last_id = max_id
            total_processed += len(raw_data)
            
            logger.info(f"✅ Batch {batch_count} completed. Processed up to ID: {max_id}")
            
            # Small delay between batches to reduce database load
            time.sleep(1)
        
        logger.info(f"🎉 BTS Coin Info cleaning completed. Total processed: {total_processed} rows in {batch_count} batches")
        
    except Exception as e:
        logger.error(f"❌ Error in BTS Coin Info cleaning: {e}")
        raise

if __name__ == "__main__":
    clean_bts_coin_info()
