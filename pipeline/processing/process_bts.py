#!/usr/bin/env python3
"""
Process BTS Transaction Data
Python equivalent of BTS.ts
"""

import sys
import os
import asyncio
import aiohttp
from decimal import Decimal
from datetime import datetime
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from pipeline_config import db_config, logger, BATCH_SIZE, SCHEMA_NAME, HELIUS_RPC_URL, HELIUS_API_KEY
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

def create_processed_bts_table():
    """Create the processed BTS table"""
    columns_definition = """
        id BIGSERIAL PRIMARY KEY,
        tokenAddress VARCHAR(255),
        buy_amount DECIMAL,
        buy_price DECIMAL,
        buy_walletAddress VARCHAR(255),
        buy_timestamp TIMESTAMP,
        buy_amountInDollars DECIMAL,
        partial_sell_amount DECIMAL,
        partial_sell_price DECIMAL,
        partial_sell_walletAddress VARCHAR(255),
        partial_sell_amountInDollars DECIMAL,
        partial_sell_timestamp TIMESTAMP,
        partial_sell_botId INTEGER,
        sell_amount DECIMAL,
        sell_price DECIMAL,
        sell_walletAddress VARCHAR(255),
        sell_timestamp TIMESTAMP,
        sell_amountInDollars DECIMAL,
        dollarProfit DECIMAL,
        profit DECIMAL,
        win_loss VARCHAR(10),
        symbol VARCHAR(100),
        name VARCHAR(255),
        btsCoinInfoId INTEGER,
        row_hash VARCHAR(64) UNIQUE,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    """
    
    create_table_if_not_exists('processed_bts', columns_definition)

async def fetch_coin_info(session, token_address):
    """Fetch coin info from Helius API"""
    try:
        if not HELIUS_RPC_URL:
            logger.warning("⚠️ HELIUS_API_KEY not configured")
            return None, None
            
        # Use Helius Enhanced Transactions API to get token metadata
        url = f"https://api.helius.xyz/v0/token-metadata?api-key={HELIUS_API_KEY}"
        payload = {
            "mintAccounts": [token_address],
            "includeOffChain": True,
            "disableCache": False
        }
        
        async with session.post(url, json=payload) as response:
            if response.status == 200:
                data = await response.json()
                if data and len(data) > 0:
                    token_data = data[0]
                    # Extract symbol and name from Helius response
                    symbol = token_data.get('onChainMetadata', {}).get('metadata', {}).get('data', {}).get('symbol')
                    name = token_data.get('onChainMetadata', {}).get('metadata', {}).get('data', {}).get('name')
                    return symbol, name
                else:
                    logger.warning(f"⚠️ No token data found for {token_address}")
                    return None, None
            else:
                logger.warning(f"⚠️ Helius API error for {token_address}: {response.status}")
                return None, None
    except Exception as e:
        logger.error(f"❌ Error fetching coin info for {token_address}: {e}")
        return None, None

def pair_bts_transactions(transactions):
    """Pair buy/sell transactions by token address"""
    # Group transactions by token address
    grouped = {}
    for tx in transactions:
        if not tx.get('tokenaddress'):
            continue
        token_address = tx['tokenaddress']
        if token_address not in grouped:
            grouped[token_address] = []
        grouped[token_address].append(tx)
    
    # Pair transactions
    paired_trades = []
    for token_address, txs in grouped.items():
        buy = next((tx for tx in txs if tx.get('type', '').lower() == 'buy'), None)
        sell = next((tx for tx in txs if tx.get('type', '').lower() == 'sell'), None)
        partial_sell = next((tx for tx in txs if tx.get('type', '').lower() == 'partial_sell'), None)
        
        if not buy or not sell:
            continue
            
        # Calculate amounts
        buy_amount = Decimal(buy.get('amount', '0'))
        buy_amount_dollars = Decimal(buy.get('amountindollars', '0'))
        
        partial_amount = Decimal(partial_sell.get('amount', '0')) if partial_sell else Decimal('0')
        partial_amount_dollars = Decimal(partial_sell.get('amountindollars', '0')) if partial_sell else Decimal('0')
        
        sell_amount = Decimal(sell.get('amount', '0'))
        sell_amount_dollars = Decimal(sell.get('amountindollars', '0'))
        
        total_sell_amount = partial_amount + sell_amount
        total_sell_amount_dollars = partial_amount_dollars + sell_amount_dollars
        
        # Calculate profit
        profit = total_sell_amount - buy_amount
        dollar_profit = total_sell_amount_dollars - buy_amount_dollars
        win_loss = 'WIN' if dollar_profit > 0 else 'LOSS'
        
        # Create paired trade record
        paired_trade = {
            'tokenaddress': token_address,
            'buy_amount': str(buy_amount),
            'buy_price': buy.get('price'),
            'buy_walletaddress': buy.get('walletaddress'),
            'buy_timestamp': buy.get('timestamp'),
            'buy_amountindollars': str(buy_amount_dollars),
            'partial_sell_amount': str(partial_amount) if partial_sell else None,
            'partial_sell_price': partial_sell.get('price') if partial_sell else None,
            'partial_sell_walletaddress': partial_sell.get('walletaddress') if partial_sell else None,
            'partial_sell_amountindollars': str(partial_amount_dollars) if partial_sell else None,
            'partial_sell_timestamp': partial_sell.get('timestamp') if partial_sell else None,
            'partial_sell_botid': partial_sell.get('btscoininfoid') if partial_sell else None,
            'sell_amount': str(total_sell_amount),
            'sell_price': sell.get('price'),
            'sell_walletaddress': sell.get('walletaddress'),
            'sell_timestamp': sell.get('timestamp'),
            'sell_amountindollars': str(total_sell_amount_dollars),
            'dollarprofit': str(dollar_profit),
            'profit': str(profit),
            'win_loss': win_loss,
            'symbol': None,  # Will be filled by API call
            'name': None,    # Will be filled by API call
            'btscoininfoid': buy.get('btscoininfoid'),
            'lastSourceId': max(buy.get('id', 0), sell.get('id', 0), 
                              partial_sell.get('id', 0) if partial_sell else 0)
        }
        
        paired_trades.append(paired_trade)
    
    return paired_trades

async def process_bts_transactions():
    """Main BTS processing function"""
    try:
        logger.info("🔄 Starting BTS transaction processing...")
        
        # Setup schema and table
        setup_processed_schema()
        create_processed_bts_table()
        
        # Get last processed ID
        last_id = get_last_processed_id('processed_bts')
        logger.info(f"📌 Starting from ID: {last_id}")
        
        total_processed = 0
        batch_count = 0
        
        while True:
            # Fetch data from raw table
            raw_data = fetch_data_with_pagination('btstransaction', last_id, BATCH_SIZE)
            
            if not raw_data:
                logger.info("⚠️ No more data to process")
                break
            
            batch_count += 1
            logger.info(f"📊 Processing batch {batch_count}: {len(raw_data)} transactions...")
            
            # Convert timestamps
            raw_data = convert_timestamps(raw_data)
            
            # Pair transactions
            paired_trades = pair_bts_transactions(raw_data)
            
            if not paired_trades:
                logger.info(f"⚠️ No paired trades found in batch {batch_count}")
                # Still update the tracker to move past this batch
                max_id = max(tx.get('id', 0) for tx in raw_data)
                update_pipeline_tracker('processed_bts', max_id)
                last_id = max_id
                continue
            
            logger.info(f"🔗 Found {len(paired_trades)} paired trades in batch {batch_count}")
            
            # Fetch coin info for all unique token addresses
            unique_tokens = list(set(trade['tokenaddress'] for trade in paired_trades))
            
            async with aiohttp.ClientSession() as session:
                # Fetch coin info for all tokens
                coin_info_tasks = [fetch_coin_info(session, token) for token in unique_tokens]
                coin_info_results = await asyncio.gather(*coin_info_tasks, return_exceptions=True)
                
                # Create token info mapping
                token_info = {}
                for token, (symbol, name) in zip(unique_tokens, coin_info_results):
                    if isinstance(symbol, Exception) or isinstance(name, Exception):
                        logger.warning(f"⚠️ Error fetching info for {token}")
                        continue
                    token_info[token] = {'symbol': symbol, 'name': name}
            
            # Update paired trades with coin info
            processed_trades = []
            for trade in paired_trades:
                token_address = trade['tokenaddress']
                coin_data = token_info.get(token_address, {})
                
                trade['symbol'] = coin_data.get('symbol')
                trade['name'] = coin_data.get('name')
                
                # Skip trades without symbol/name
                if not trade['symbol'] or not trade['name']:
                    logger.warning(f"⚠️ Skipping {token_address} due to missing name/symbol")
                    continue
                
                # Generate row hash
                trade['row_hash'] = generate_row_hash(trade)
                processed_trades.append(trade)
            
            if not processed_trades:
                logger.info(f"⚠️ No valid trades after coin info filtering in batch {batch_count}")
                # Still update the tracker to move past this batch
                max_id = max(trade['lastSourceId'] for trade in paired_trades)
                update_pipeline_tracker('processed_bts', max_id)
                last_id = max_id
                continue
            
            # Insert processed trades
            columns = [
                'tokenaddress', 'buy_amount', 'buy_price', 'buy_walletaddress', 'buy_timestamp',
                'buy_amountindollars', 'partial_sell_amount', 'partial_sell_price', 
                'partial_sell_walletaddress', 'partial_sell_amountindollars', 'partial_sell_timestamp',
                'partial_sell_botid', 'sell_amount', 'sell_price', 'sell_walletaddress',
                'sell_timestamp', 'sell_amountindollars', 'dollarprofit', 'profit', 'win_loss',
                'symbol', 'name', 'btscoininfoid', 'row_hash'
            ]
            
            batch_insert('processed_bts', processed_trades, columns)
            
            # Update tracker and last_id for next batch
            max_id = max(trade['lastSourceId'] for trade in processed_trades)
            update_pipeline_tracker('processed_bts', max_id)
            last_id = max_id
            total_processed += len(processed_trades)
            
            logger.info(f"✅ Batch {batch_count} completed. Processed up to ID: {max_id}")
        
        logger.info(f"🎉 BTS processing completed. Total processed: {total_processed} trades in {batch_count} batches")
        
    except Exception as e:
        logger.error(f"❌ Error in BTS processing: {e}")
        raise

if __name__ == "__main__":
    asyncio.run(process_bts_transactions())
