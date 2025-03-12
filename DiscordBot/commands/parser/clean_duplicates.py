"""
Script to clean up duplicate entries in parsed files.
"""

import asyncio
import os
import sys
import logging
from typing import List

# Add the parent directory to the path so we can import our modules
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../..')))

from commands.parser.utils import alphabetize_file
from commands.parser.config import get_config

logging.basicConfig(level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

async def clean_parsed_files(months: List[str] = None):
    """
    Clean up duplicate entries in parsed files for specified months
    
    Args:
        months: List of month names to process, or None for all available months
    """
    config = get_config()
    textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
    
    if not textfiles_dir:
        logger.error("Textfiles directory not found in config")
        return
    
    # If no months specified, get all month directories
    if not months:
        months = []
        for item in os.listdir(textfiles_dir):
            if os.path.isdir(os.path.join(textfiles_dir, item)):
                months.append(item)
    
    logger.info(f"Processing months: {', '.join(months)}")
    
    for month in months:
        month_dir = os.path.join(textfiles_dir, month)
        month_lower = month.lower()
        
        # Files to clean
        files = [
            os.path.join(month_dir, f'parsed_{month_lower}.txt'),
            os.path.join(month_dir, f'parse_queue_{month_lower}.txt'),
            os.path.join(month_dir, f'downloaded_{month_lower}.txt'),
            os.path.join(month_dir, f'download_queue_{month_lower}.txt')
        ]
        
        for file_path in files:
            if os.path.exists(file_path):
                logger.info(f"Cleaning duplicates in {file_path}")
                await alphabetize_file(file_path, remove_duplicates=True)
            else:
                logger.warning(f"File not found: {file_path}")

async def main():
    """Main entry point"""
    # Get months from command line arguments, or use None for all months
    months = sys.argv[1:] if len(sys.argv) > 1 else None
    await clean_parsed_files(months)
    logger.info("Duplicate cleanup complete")

if __name__ == "__main__":
    asyncio.run(main())
