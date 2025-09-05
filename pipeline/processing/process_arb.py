#!/usr/bin/env python3
"""
Process Arbitrage Transaction Data
Python equivalent of Arb.ts
"""

import sys
import os
from decimal import Decimal
from datetime import datetime
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
    fetch_data_with_pagination,
    convert_timestamps
)

def create_processed_arb_table():
    """Create the processed arbitrage table"""
    columns_definition = """
        id BIGSERIAL PRIMARY KEY,
        tokenAddress VARCHAR(255),
        buyExchange VARCHAR(100),
        sellExchange VARCHAR(100),
        buyAmount DECIMAL,
        sellAmount DECIMAL,
        buyPrice DECIMAL,
        sellPrice DECIMAL,
        buyTimestamp TIMESTAMP,
        sellTimestamp TIMESTAMP,
        profit DECIMAL,
        profitPercentage DECIMAL,
        win_loss VARCHAR(10),
        symbol VARCHAR(100),
        name VARCHAR(255),
        row_hash VARCHAR(64) UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    """
    
    create_table_if_not_exists('processed_arb', columns_definition)

def process_arb_transactions():
    """Process arbitrage transaction data"""
    try:
        logger.info("🔄 Starting Arbitrage transaction processing...")
        
        # Setup schema and table
        setup_processed_schema()
        create_processed_arb_table()
        
        # Get last processed ID
        last_id = get_last_processed_id('processed_arb')
        logger.info(f"📌 Starting from ID: {last_id}")
        
        total_processed = 0
        batch_count = 0
        
        while True:
            # Fetch data from raw table
            raw_data = fetch_data_with_pagination('arbtransaction', last_id, BATCH_SIZE)
            
            if not raw_data:
                logger.info("⚠️ No more data to process")
                break
            
            batch_count += 1
            logger.info(f"📊 Processing batch {batch_count}: {len(raw_data)} arbitrage transactions...")
            
            # Convert timestamps
            raw_data = convert_timestamps(raw_data)
            
            # Process arbitrage transactions
            processed_transactions = []
            for tx in raw_data:
                # Map to actual arbtransaction table field names
                buy_amount = Decimal(tx.get('buybase', '0'))
                sell_amount = Decimal(tx.get('sellbase', '0'))
                buy_price = Decimal(tx.get('buyvwap', '0'))
                sell_price = Decimal(tx.get('sellvwap', '0'))
                buy_volume = Decimal(tx.get('buyvolume', '0'))
                sell_volume = Decimal(tx.get('sellvolume', '0'))
                
                # Calculate profit from idealProfit field
                ideal_profit = Decimal(tx.get('idealprofit', '0'))
                profit = ideal_profit
                profit_percentage = (profit / abs(buy_volume) * 100) if buy_volume != 0 else 0
                win_loss = 'WIN' if profit > 0 else 'LOSS'
                
                # Create processed transaction record
                processed_tx = {
                    'tokenaddress': tx.get('buybase'),  # Use buyBase as token address (e.g., TBC)
                    'buyexchange': tx.get('buyexchange'),
                    'sellexchange': tx.get('sellexchange'),
                    'buyamount': str(buy_amount),
                    'sellamount': str(sell_amount),
                    'buyprice': str(buy_price),
                    'sellprice': str(sell_price),
                    'buytimestamp': tx.get('datetraded'),
                    'selltimestamp': tx.get('datetraded'),
                    'profit': str(profit),
                    'profitpercentage': str(profit_percentage),
                    'win_loss': win_loss,
                    'symbol': tx.get('buyquote'),  # Use buyQuote as symbol (e.g., USDT)
                    'name': tx.get('buybase'),     # Use buyBase as name (e.g., TBC)
                    'row_hash': generate_row_hash({
                        'id': tx.get('id'),
                        'datetraded': tx.get('datetraded'),
                        'buybase': tx.get('buybase'),
                        'buyquote': tx.get('buyquote'),
                        'buyexchange': tx.get('buyexchange'),
                        'sellexchange': tx.get('sellexchange'),
                        'idealprofit': str(ideal_profit),
                        'botid': tx.get('botid')
                    })
                }
                
                processed_transactions.append(processed_tx)
            
            if not processed_transactions:
                logger.info(f"⚠️ No valid arbitrage transactions found in batch {batch_count}")
                # Still update the tracker to move past this batch
                max_id = max(tx.get('id', 0) for tx in raw_data)
                update_pipeline_tracker('processed_arb', max_id)
                last_id = max_id
                continue
            
            # Insert processed transactions
            columns = [
                'tokenaddress', 'buyexchange', 'sellexchange', 'buyamount', 'sellamount',
                'buyprice', 'sellprice', 'buytimestamp', 'selltimestamp', 'profit',
                'profitpercentage', 'win_loss', 'symbol', 'name', 'row_hash'
            ]
            
            batch_insert('processed_arb', processed_transactions, columns)
            
            # Update tracker and last_id for next batch
            max_id = max(tx.get('id', 0) for tx in raw_data)
            update_pipeline_tracker('processed_arb', max_id)
            last_id = max_id
            total_processed += len(processed_transactions)
            
            logger.info(f"✅ Batch {batch_count} completed. Processed up to ID: {max_id}")
        
        logger.info(f"🎉 Arbitrage processing completed. Total processed: {total_processed} transactions in {batch_count} batches")
        
    except Exception as e:
        logger.error(f"❌ Error in Arbitrage processing: {e}")
        raise

if __name__ == "__main__":
    process_arb_transactions()
