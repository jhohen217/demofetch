"""
Configuration handling for the scraper module.
"""

import os
import json
from typing import Dict, Any, List, Optional

# Default scraping delay settings (in seconds)
DEFAULT_MIN_DELAY = 180  # 3 minutes
DEFAULT_MAX_DELAY = 300  # 5 minutes

def get_config() -> Dict[str, Any]:
    """
    Load configuration from the project's config.json file.
    
    Returns:
        Dict[str, Any]: Configuration dictionary
    """
    # Load configuration from project root
    core_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # DiscordBot directory
    config_path = os.path.join(os.path.dirname(core_dir), 'config.json')
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Ensure fetch delay settings exist with reasonable defaults
    if 'downloader' not in config:
        config['downloader'] = {}
    
    if 'fetch_delay' not in config['downloader']:
        config['downloader']['fetch_delay'] = {}
    
    # Set default values if not present
    if 'min' not in config['downloader']['fetch_delay']:
        config['downloader']['fetch_delay']['min'] = DEFAULT_MIN_DELAY
    
    if 'max' not in config['downloader']['fetch_delay']:
        config['downloader']['fetch_delay']['max'] = DEFAULT_MAX_DELAY
    
    # Ensure values are reasonable
    min_delay = config['downloader']['fetch_delay']['min']
    max_delay = config['downloader']['fetch_delay']['max']
    
    if min_delay < 60:  # Minimum 1 minute
        print(f"Warning: Minimum delay {min_delay}s is too low. Setting to {DEFAULT_MIN_DELAY}s.")
        config['downloader']['fetch_delay']['min'] = DEFAULT_MIN_DELAY
    
    if max_delay < min_delay:
        print(f"Warning: Maximum delay {max_delay}s is less than minimum delay {min_delay}s. Setting to {min_delay + 120}s.")
        config['downloader']['fetch_delay']['max'] = min_delay + 120
    
    return config

def get_available_months() -> List[str]:
    """
    Get a list of available months based on the textfiles directory structure.
    
    Returns:
        List[str]: List of month names (e.g., ["January", "February"])
    """
    config = get_config()
    textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
    
    if not os.path.exists(textfiles_dir):
        return []
    
    # Get all directories in the textfiles directory
    months = []
    for item in os.listdir(textfiles_dir):
        if os.path.isdir(os.path.join(textfiles_dir, item)):
            # Check if it's a valid month name
            if item.lower() in [
                'january', 'february', 'march', 'april', 'may', 'june',
                'july', 'august', 'september', 'october', 'november', 'december'
            ]:
                months.append(item)
    
    return months

def get_month_files(month: str) -> Dict[str, str]:
    """
    Get file paths for a specific month.
    
    Args:
        month: Month name (e.g., "February")
        
    Returns:
        Dict[str, str]: Dictionary of file paths
    """
    config = get_config()
    textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
    
    if not textfiles_dir:
        return {}
    
    month_dir = os.path.join(textfiles_dir, month)
    month_lower = month.lower()
    
    return {
        'output_json': os.path.join(month_dir, f"output_{month_lower}.json"),
        'match_ids_file': os.path.join(month_dir, f"match_ids_{month_lower}.txt"),
        'permanent_fail_file': os.path.join(month_dir, f"permanent_fails_{month_lower}.txt"),
        'hub_output_json': os.path.join(month_dir, f"hub_output_{month_lower}.json"),
    }

def save_config(config: Dict[str, Any]) -> bool:
    """
    Save configuration to the project's config.json file.
    
    Args:
        config: Configuration dictionary
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Load configuration from project root
        core_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(core_dir), 'config.json')
        
        with open(config_path, 'w') as f:
            json.dump(config, f, indent=4)
        
        return True
    except Exception as e:
        print(f"Error saving config: {str(e)}")
        return False

def update_fetch_delay(min_delay: int, max_delay: int) -> bool:
    """
    Update the fetch delay settings in the configuration.
    
    Args:
        min_delay: Minimum delay between scrapes in seconds
        max_delay: Maximum delay between scrapes in seconds
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        config = get_config()
        
        # Ensure values are reasonable
        if min_delay < 60:  # Minimum 1 minute
            print(f"Warning: Minimum delay {min_delay}s is too low. Setting to {DEFAULT_MIN_DELAY}s.")
            min_delay = DEFAULT_MIN_DELAY
        
        if max_delay < min_delay:
            print(f"Warning: Maximum delay {max_delay}s is less than minimum delay {min_delay}s. Setting to {min_delay + 120}s.")
            max_delay = min_delay + 120
        
        # Update config
        config['downloader']['fetch_delay']['min'] = min_delay
        config['downloader']['fetch_delay']['max'] = max_delay
        
        # Save config
        return save_config(config)
    except Exception as e:
        print(f"Error updating fetch delay: {str(e)}")
        return False
