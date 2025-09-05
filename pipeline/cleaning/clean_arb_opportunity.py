#!/usr/bin/env python3
"""
Clean Arbitrage Opportunity Data
Python equivalent of cleanArbOpportunity.ts
"""

import sys
import os
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import sys
import os
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

def create_arb_opportunity_table():
    """Create the cleaned arbitrage opportunity table"""
    columns_definition = """
        id BIGINT PRIMARY KEY,
        symbol VARCHAR(100),
        stableSymbol VARCHAR(100),
        minExchange VARCHAR(100),
        maxExchange VARCHAR(100),
        profit DECIMAL,
        timestamp TIMESTAMP,
        row_hash VARCHAR(64) UNIQUE
    """
    
    create_table_if_not_exists('clean_arb_opportunity', columns_definition)

def clean_arb_opportunity():
    """Clean and copy arbitrage opportunity data"""
    try:
        logger.info("🔄 Starting Arbitrage Opportunity cleaning process...")
        
        # Setup schema and table
        setup_processed_schema()
        create_arb_opportunity_table()
        
        # Get last processed ID
        last_id = get_last_processed_id('clean_arb_opportunity')
        logger.info(f"📌 Starting from ID: {last_id}")
        
        total_processed = 0
        batch_count = 0
        
        while True:
            # Fetch data from raw table
            raw_data = fetch_data_with_pagination('arbopportunity', last_id, BATCH_SIZE)
            
            if not raw_data:
                logger.info("⚠️ No more data to process")
                break
            
            batch_count += 1
            logger.info(f"📊 Processing batch {batch_count}: {len(raw_data)} rows...")
            
            # Process data
            processed_data = []
            for row in raw_data:
                # Create processed row (excluding row_hash)
                processed_row = {}
                for k, v in row.items():
                    if k.lower() != 'row_hash':
                        # Handle empty strings for numeric fields
                        if k.lower() in ['profit']:
                            processed_row[k.lower()] = v if v and str(v).strip() else None
                        else:
                            processed_row[k.lower()] = v
                
                # Generate new row hash
                processed_row['row_hash'] = generate_row_hash(processed_row)
                processed_data.append(processed_row)
            
            # Insert processed data
            columns = list(processed_data[0].keys())
            batch_insert('clean_arb_opportunity', processed_data, columns)
            
            # Update tracker and last_id for next batch
            max_id = max(row['id'] for row in raw_data)
            update_pipeline_tracker('clean_arb_opportunity', max_id)
            last_id = max_id
            total_processed += len(raw_data)
            
            logger.info(f"✅ Batch {batch_count} completed. Processed up to ID: {max_id}")
        
        logger.info(f"🎉 Arbitrage Opportunity cleaning completed. Total processed: {total_processed} rows in {batch_count} batches")
        
    except Exception as e:
        logger.error(f"❌ Error in Arbitrage Opportunity cleaning: {e}")
        raise

if __name__ == "__main__":
    clean_arb_opportunity()
