"""
Test script to demonstrate the optimized parsing setup.
"""

import os
import asyncio
import logging
from commands.parser.logger_config import configure_loggers
from commands.parser.config import get_config
from commands.parser.queue_manager import prepare_parse_queue_async

async def test_parse_queue_preparation(month: str):
    """
    Test the parse queue preparation process for a specific month.
    
    Args:
        month: Month name (e.g., "December")
    """
    print(f"\n{'='*50}")
    print(f"Testing parse queue preparation for {month}")
    print(f"{'='*50}\n")
    
    # Configure loggers
    main_logger, debug_logger = configure_loggers()
    
    # Get configuration
    config = get_config()
    
    # Create a stop event
    stop_event = asyncio.Event()
    
    # Prepare parse queue
    print(f"Preparing parse queue for {month}...\n")
    success, stats = await prepare_parse_queue_async(month, config, stop_event)
    
    if success:
        print(f"\nParse queue preparation completed successfully!")
        print(f"Stats:")
        print(f"  - Total downloaded: {stats['total_downloaded']}")
        print(f"  - Already parsed: {stats['already_parsed']}")
        print(f"  - Unprocessed: {stats['unprocessed']}")
        print(f"  - Unarchived demos: {stats['unarchived_demos']}")
        print(f"  - Queue size: {stats['queue_size']}")
        
        # Show path to debug log
        logs_dir = os.path.join(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))), 'logs')
        debug_log_path = os.path.join(logs_dir, 'debug.log')
        print(f"\nDetailed logs written to: {debug_log_path}")
    else:
        print(f"\nParse queue preparation failed!")
        if 'error' in stats:
            print(f"Error: {stats['error']}")

async def main():
    """Main function to run the test."""
    # Test with December
    await test_parse_queue_preparation("December")
    
    # Uncomment to test with other months
    # await test_parse_queue_preparation("February")

if __name__ == "__main__":
    # Run the test
    asyncio.run(main())
