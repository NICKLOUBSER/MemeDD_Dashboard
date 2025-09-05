#!/usr/bin/env python3
"""
Pipeline Runner
Main script to run individual pipeline processes or all processes
"""

import argparse
import sys
import os
from typing import Dict, Callable
import importlib.util

# Add current directory to path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from .pipeline_config import logger
from .pipeline_utils import setup_processed_schema

# Define available processes
PROCESSES = {
    'clean_bts_coin_info': {
        'module': 'cleaning.clean_bts_coin_info',
        'function': 'clean_bts_coin_info',
        'description': 'Clean BTS coin info data'
    },
    'clean_arb_opportunity': {
        'module': 'cleaning.clean_arb_opportunity',
        'function': 'clean_arb_opportunity',
        'description': 'Clean arbitrage opportunity data'
    },
    'process_bts': {
        'module': 'processing.process_bts',
        'function': 'process_bts_transactions',
        'description': 'Process BTS transaction data'
    },
    'process_arb': {
        'module': 'processing.process_arb',
        'function': 'process_arb_transactions',
        'description': 'Process arbitrage transaction data'
    }
}

def load_process_function(process_name: str) -> Callable:
    """Load a process function from its module"""
    if process_name not in PROCESSES:
        raise ValueError(f"Unknown process: {process_name}")
    
    process_info = PROCESSES[process_name]
    module_name = process_info['module']
    function_name = process_info['function']
    
    # Import the module
    spec = importlib.util.spec_from_file_location(
        module_name, 
        os.path.join(os.path.dirname(__file__), f"{module_name.replace('.', '/')}.py")
    )
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module) # type: ignore
    
    # Get the function
    return getattr(module, function_name)

def run_process(process_name: str):
    """Run a single process"""
    try:
        logger.info(f"🚀 Starting process: {process_name}")
        
        # Load and run the process
        process_function = load_process_function(process_name)
        
        # Check if it's an async function
        if asyncio.iscoroutinefunction(process_function):
            asyncio.run(process_function())
        else:
            process_function()
            
        logger.info(f"✅ Process completed: {process_name}")
        
    except Exception as e:
        logger.error(f"❌ Process failed: {process_name} - {e}")
        raise

def run_all_processes():
    """Run all processes in order"""
    try:
        logger.info("🚀 Starting all pipeline processes...")
        
        # Setup schema first
        setup_processed_schema()
        
        # Run processes in order
        for process_name in PROCESSES.keys():
            logger.info(f"🔄 Running {process_name}...")
            run_process(process_name)
            logger.info(f"✅ {process_name} completed")
        
        logger.info("🎉 All processes completed successfully!")
        
    except Exception as e:
        logger.error(f"❌ Pipeline failed: {e}")
        raise

def list_processes():
    """List all available processes"""
    print("Available processes:")
    print("-" * 50)
    for name, info in PROCESSES.items():
        print(f"{name}: {info['description']}")
    print("-" * 50)

def main():
    parser = argparse.ArgumentParser(description='Pipeline Runner')
    parser.add_argument(
        'process', 
        nargs='?',
        choices=list(PROCESSES.keys()) + ['all', 'list'],
        help='Process to run (or "all" for all processes, "list" to list available processes)'
    )
    
    args = parser.parse_args()
    
    if not args.process:
        parser.print_help()
        return
    
    if args.process == 'list':
        list_processes()
        return
    
    if args.process == 'all':
        run_all_processes()
    else:
        run_process(args.process)

if __name__ == "__main__":
    import asyncio
    main()
