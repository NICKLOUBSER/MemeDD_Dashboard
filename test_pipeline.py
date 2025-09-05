#!/usr/bin/env python3
"""
Test the updated pipeline with multi-batch processing
"""

import sys
import os

# Add the pipeline directory to the path
sys.path.append(os.path.join(os.path.dirname(__file__), 'pipeline'))

def test_pipeline():
    """Test the pipeline with a smaller batch size for testing"""
    try:
        # Import and modify batch size for testing
        from pipeline.pipeline_config import BATCH_SIZE
        original_batch_size = BATCH_SIZE
        
        # Set a smaller batch size for testing
        import pipeline.pipeline_config
        pipeline.pipeline_config.BATCH_SIZE = 1000
        
        print(f"Testing pipeline with batch size: {pipeline.pipeline_config.BATCH_SIZE}")
        print("This will process all data in batches until completion...")
        
        # Run the pipeline
        from pipeline.run_pipeline import main
        main()
        
        # Restore original batch size
        pipeline.pipeline_config.BATCH_SIZE = original_batch_size
        
    except Exception as e:
        print(f"Pipeline test failed: {e}")
        raise

if __name__ == "__main__":
    test_pipeline()
