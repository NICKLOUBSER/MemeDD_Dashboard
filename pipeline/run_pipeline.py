#!/usr/bin/env python3
"""
Simple Pipeline Runner
Run this script from the pipeline directory
"""

import sys
import os
import asyncio

# Add current directory to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from pipeline_config import logger
from pipeline_utils import setup_processed_schema

def run_clean_bts_coin_info():
    """Run BTS coin info cleaning"""
    from cleaning.clean_bts_coin_info import clean_bts_coin_info
    clean_bts_coin_info()

def run_clean_arb_opportunity():
    """Run arbitrage opportunity cleaning"""
    from cleaning.clean_arb_opportunity import clean_arb_opportunity
    clean_arb_opportunity()

async def run_process_bts():
    """Run BTS processing"""
    from processing.process_bts import process_bts_transactions
    await process_bts_transactions()

def run_process_arb():
    """Run arbitrage processing"""
    from processing.process_arb import process_arb_transactions
    process_arb_transactions()

def main():
    """Main function to run all processes"""
    try:
        logger.info("🚀 Starting pipeline...")
        
        # Setup schema first
        setup_processed_schema()
        
        # Run cleaning processes
        logger.info("🔄 Running cleaning processes...")
        run_clean_bts_coin_info()
        run_clean_arb_opportunity()
        
        # Run processing processes
        logger.info("🔄 Running processing processes...")
        asyncio.run(run_process_bts())
        run_process_arb()
        
        logger.info("🎉 Pipeline completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise

if __name__ == "__main__":
    main()
