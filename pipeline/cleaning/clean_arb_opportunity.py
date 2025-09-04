#!/usr/bin/env python3
"""
Clean Arbitrage Opportunity Data
Python equivalent of cleanArbOpportunity.ts
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

def create_arb_opportunity_table():
    """Create the cleaned arbitrage opportunity table"""
    columns_definition = """
        id BIGINT PRIMARY KEY,
        tokenAddress VARCHAR(255),
        buyExchange VARCHAR(100),
        sellExchange VARCHAR(100),
        buyPrice DECIMAL,
        sellPrice DECIMAL,
        priceDifference DECIMAL,
        profitPercentage DECIMAL,
        volume DECIMAL,
        liquidity DECIMAL,
        timestamp TIMESTAMP,
        status VARCHAR(50),
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
        
        # Fetch data from raw table
        raw_data = fetch_data_with_pagination('arbopportunity', last_id, BATCH_SIZE)
        
        if not raw_data:
            logger.info("⚠️ No new data to process")
            return
        
        logger.info(f"📊 Processing {len(raw_data)} rows...")
        
        # Process data
        processed_data = []
        for row in raw_data:
            # Create processed row (excluding row_hash)
            processed_row = {k: v for k, v in row.items() 
                           if k.lower() != 'row_hash'}
            
            # Generate new row hash
            processed_row['row_hash'] = generate_row_hash(processed_row)
            processed_data.append(processed_row)
        
        # Insert processed data
        columns = list(processed_data[0].keys())
        batch_insert('clean_arb_opportunity', processed_data, columns)
        
        # Update tracker
        max_id = max(row['id'] for row in raw_data)
        update_pipeline_tracker('clean_arb_opportunity', max_id)
        
        logger.info(f"✅ Arbitrage Opportunity cleaning completed. Processed up to ID: {max_id}")
        
    except Exception as e:
        logger.error(f"❌ Error in Arbitrage Opportunity cleaning: {e}")
        raise

if __name__ == "__main__":
    clean_arb_opportunity()
