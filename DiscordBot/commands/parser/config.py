"""
Configuration functions for the parser module.
"""

import os
import json
import logging
import asyncio
from typing import Dict, Optional, List

logger = logging.getLogger('discord_bot')

def get_config() -> Dict:
    """Load configuration from config.json"""
    try:
        core_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(core_dir), 'config.json')
        
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        logger.error(f"Error loading config: {str(e)}")
        return {}

def get_month_files(month: str) -> Optional[Dict[str, str]]:
    """Get file paths for a specific month"""
    config = get_config()
    textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
    
    if not textfiles_dir:
        logger.error("Textfiles directory not found in config")
        return None
    
    month_dir = os.path.join(textfiles_dir, month)
    month_lower = month.lower()
    
    return {
        'dir': month_dir,
        'downloaded': os.path.join(month_dir, f'downloaded_{month_lower}.txt'),
        'parsed': os.path.join(month_dir, f'parsed_{month_lower}.txt'),
        'parse_queue': os.path.join(month_dir, f'parse_queue_{month_lower}.txt')
    }

def get_available_months() -> List[str]:
    """Get list of available months with downloaded files"""
    config = get_config()
    textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
    
    if not textfiles_dir or not os.path.exists(textfiles_dir):
        logger.error(f"Textfiles directory not found: {textfiles_dir}")
        return []
    
    months = []
    try:
        for item in os.listdir(textfiles_dir):
            try:
                item_path = os.path.join(textfiles_dir, item)
                if os.path.isdir(item_path) and item not in ['undated', 'MergeMe', 'System Volume Information']:
                    # Skip system directories explicitly
                    if item.lower() == 'system volume information':
                        continue
                        
                    # Check if there's a downloaded_{month}.txt file
                    month_lower = item.lower()
                    downloaded_file = os.path.join(item_path, f'downloaded_{month_lower}.txt')
                    if os.path.exists(downloaded_file):
                        months.append(item)
            except PermissionError as e:
                # Handle access denied errors for specific subdirectories
                logger.warning(f"Permission denied when checking directory {item_path}: {str(e)}")
            except Exception as e:
                logger.error(f"Error checking directory {item_path}: {str(e)}")
    except PermissionError as e:
        # Handle access denied errors for the main directory
        logger.error(f"Permission denied when listing textfiles directory {textfiles_dir}: {str(e)}")
    except Exception as e:
        logger.error(f"Error listing textfiles directory {textfiles_dir}: {str(e)}")
    
    return months

def get_demo_path(demo_id: str, month: str) -> Optional[str]:
    """
    Get the full path to a demo file
    
    Args:
        demo_id: Demo ID (e.g., "12-05-24_0101_1-72d8eb18-0bc8-49af-a738-55b792248b79" or "1-72d8eb18-0bc8-49af-a738-55b792248b79")
        month: Month name (e.g., "February")
        
    Returns:
        Optional[str]: Full path to the demo file, or None if not found
    """
    config = get_config()
    demos_dir = config.get('project', {}).get('public_demos_directory', '')
    
    if not demos_dir:
        logger.error("Public demos directory not found in config")
        return None
    
    # Import the extract_uuid_from_demo_id function from utils
    from commands.parser.utils import extract_uuid_from_demo_id
    
    # Extract the UUID using the improved function from utils
    match_id = extract_uuid_from_demo_id(demo_id)
    
    # Log the extracted match ID for debugging (using debug_logger instead of logger)
    debug_logger = logging.getLogger('debug_discord_bot')
    debug_logger.info(f"Extracted match ID: {match_id} from demo ID: {demo_id}")
    
    # Define possible directory locations to check
    directories_to_check = []
    
    # Special case for December which might be stored in different locations
    if month == "December":
        directories_to_check.extend([
            os.path.join(demos_dir, "DecemberMode"),
            os.path.join(demos_dir, "December"),
            demos_dir  # Root directory
        ])
    else:
        directories_to_check.extend([
            os.path.join(demos_dir, month),
            demos_dir  # Root directory
        ])
    
    # Check all possible locations for the demo file
    for directory in directories_to_check:
        try:
            if not os.path.exists(directory):
                continue
                
            # Check for the demo file with different extensions and naming patterns
            possible_filenames = [
                f"{match_id}.dem.gz",  # Compressed
                f"{match_id}.dem",      # Uncompressed
            ]
            
            for filename in possible_filenames:
                demo_path = os.path.join(directory, filename)
                if os.path.exists(demo_path):
                    debug_logger.info(f"Found demo file at: {demo_path}")
                    return demo_path
                    
            # If not found with direct match, try to find by partial match
            if os.path.exists(directory):
                for filename in os.listdir(directory):
                    if match_id in filename and (filename.endswith('.dem') or filename.endswith('.dem.gz')):
                        demo_path = os.path.join(directory, filename)
                        debug_logger.info(f"Found demo file by partial match at: {demo_path}")
                        return demo_path
                        
        except PermissionError as e:
            logger.warning(f"Permission denied when checking directory {directory}: {str(e)}")
        except Exception as e:
            logger.error(f"Error checking directory {directory}: {str(e)}")
    
    logger.warning(f"Demo file not found for ID: {demo_id}")
    return None

async def get_demo_path_async(demo_id: str, month: str) -> Optional[str]:
    """
    Get the full path to a demo file asynchronously
    
    Args:
        demo_id: Demo ID (e.g., "12-05-24_0101_1-72d8eb18-0bc8-49af-a738-55b792248b79" or "1-72d8eb18-0bc8-49af-a738-55b792248b79")
        month: Month name (e.g., "February")
        
    Returns:
        Optional[str]: Full path to the demo file, or None if not found
    """
    # For now, we'll use the synchronous version since file operations are relatively fast
    # and the async benefit would be minimal. This can be optimized later if needed.
    return get_demo_path(demo_id, month)
