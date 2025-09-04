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

from ..pipeline_config import db_config, logger, BATCH_SIZE, SCHEMA_NAME
from ..pipeline_utils import (
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
        
        # Fetch data from raw table
        raw_data = fetch_data_with_pagination('arbtransaction', last_id, BATCH_SIZE)
        
        if not raw_data:
            logger.info("⚠️ No new data to process")
            return
        
        logger.info(f"📊 Processing {len(raw_data)} arbitrage transactions...")
        
        # Convert timestamps
        raw_data = convert_timestamps(raw_data)
        
        # Process arbitrage transactions
        processed_transactions = []
        for tx in raw_data:
            # Calculate profit
            buy_amount = Decimal(tx.get('buyamount', '0'))
            sell_amount = Decimal(tx.get('sellamount', '0'))
            buy_price = Decimal(tx.get('buyprice', '0'))
            sell_price = Decimal(tx.get('sellprice', '0'))
            
            profit = sell_amount - buy_amount
            profit_percentage = (profit / buy_amount * 100) if buy_amount > 0 else 0
            win_loss = 'WIN' if profit > 0 else 'LOSS'
            
            # Create processed transaction record
            processed_tx = {
                'tokenAddress': tx.get('tokenaddress'),
                'buyExchange': tx.get('buyexchange'),
                'sellExchange': tx.get('sellexchange'),
                'buyAmount': str(buy_amount),
                'sellAmount': str(sell_amount),
                'buyPrice': str(buy_price),
                'sellPrice': str(sell_price),
                'buyTimestamp': tx.get('buytimestamp'),
                'sellTimestamp': tx.get('selltimestamp'),
                'profit': str(profit),
                'profitPercentage': str(profit_percentage),
                'win_loss': win_loss,
                'symbol': tx.get('symbol'),
                'name': tx.get('name'),
                'row_hash': generate_row_hash({
                    'tokenAddress': tx.get('tokenaddress'),
                    'buyAmount': str(buy_amount),
                    'sellAmount': str(sell_amount),
                    'buyTimestamp': tx.get('buytimestamp'),
                    'sellTimestamp': tx.get('selltimestamp'),
                    'profit': str(profit)
                })
            }
            
            processed_transactions.append(processed_tx)
        
        if not processed_transactions:
            logger.info("⚠️ No valid arbitrage transactions found")
            return
        
        # Insert processed transactions
        columns = [
            'tokenAddress', 'buyExchange', 'sellExchange', 'buyAmount', 'sellAmount',
            'buyPrice', 'sellPrice', 'buyTimestamp', 'sellTimestamp', 'profit',
            'profitPercentage', 'win_loss', 'symbol', 'name', 'row_hash'
        ]
        
        batch_insert('processed_arb', processed_transactions, columns)
        
        # Update tracker with max ID
        max_id = max(tx.get('id', 0) for tx in raw_data)
        update_pipeline_tracker('processed_arb', max_id)
        
        logger.info(f"✅ Arbitrage processing completed. Processed up to ID: {max_id}")
        
    except Exception as e:
        logger.error(f"❌ Error in Arbitrage processing: {e}")
        raise

if __name__ == "__main__":
    process_arb_transactions()
