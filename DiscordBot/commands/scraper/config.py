"""
Configuration handling for the scraper module.
"""

import os
import configparser
from typing import Dict, Any, List

# Default scraping delay settings (in seconds)
DEFAULT_MIN_DELAY = 180  # 3 minutes
DEFAULT_MAX_DELAY = 300  # 5 minutes

def get_config() -> configparser.ConfigParser:
    """
    Load configuration from the project's config.ini file.
    
    Returns:
        configparser.ConfigParser: Configuration object
    """
    # Load configuration from project root
    core_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
    config_path = os.path.join(os.path.dirname(core_dir), 'config.ini')
    
    config = configparser.ConfigParser()
    config.read(config_path)
    
    # Ensure fetch delay settings exist with reasonable defaults
    if not config.has_section('Downloader'):
        config.add_section('Downloader')
    
    if not config.has_option('Downloader', 'min_fetch_delay'):
        config.set('Downloader', 'min_fetch_delay', str(DEFAULT_MIN_DELAY))
    
    if not config.has_option('Downloader', 'max_fetch_delay'):
        config.set('Downloader', 'max_fetch_delay', str(DEFAULT_MAX_DELAY))
    
    # Ensure values are reasonable
    min_delay = config.getint('Downloader', 'min_fetch_delay')
    max_delay = config.getint('Downloader', 'max_fetch_delay')
    
    if min_delay < 60:  # Minimum 1 minute
        print(f"Warning: Minimum delay {min_delay}s is too low. Setting to {DEFAULT_MIN_DELAY}s.")
        config.set('Downloader', 'min_fetch_delay', str(DEFAULT_MIN_DELAY))
    
    if max_delay < min_delay:
        print(f"Warning: Maximum delay {max_delay}s is less than minimum delay {min_delay}s. Setting to {min_delay + 120}s.")
        config.set('Downloader', 'max_fetch_delay', str(min_delay + 120))
    
    return config

# Valid lowercase month names
_MONTH_NAMES = [
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december'
]

def get_available_months() -> List[str]:
    """
    Get a list of available month folders based on the textfiles directory structure.
    Supports both legacy plain-name format (e.g. "February") and MonthYY format
    (e.g. "February26").

    Returns:
        List[str]: List of folder names
    """
    config = get_config()
    textfiles_dir = config.get('Paths', 'textfiles_directory', fallback='')

    if not os.path.exists(textfiles_dir):
        return []

    months = []
    for item in os.listdir(textfiles_dir):
        if not os.path.isdir(os.path.join(textfiles_dir, item)):
            continue
        item_lower = item.lower()
        for m in _MONTH_NAMES:
            if item_lower == m:
                months.append(item)
                break
            if item_lower.startswith(m) and len(item_lower) == len(m) + 2:
                suffix = item_lower[len(m):]
                if suffix.isdigit():
                    months.append(item)
                    break

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
    textfiles_dir = config.get('Paths', 'textfiles_directory', fallback='')
    
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

def save_config(config: configparser.ConfigParser) -> bool:
    """
    Save configuration to the project's config.ini file.
    
    Args:
        config: Configuration object
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Load configuration from project root
        core_dir = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
        config_path = os.path.join(os.path.dirname(core_dir), 'config.ini')
        
        with open(config_path, 'w') as f:
            config.write(f)
        
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
        config.set('Downloader', 'min_fetch_delay', str(min_delay))
        config.set('Downloader', 'max_fetch_delay', str(max_delay))
        
        # Save config
        return save_config(config)
    except Exception as e:
        print(f"Error updating fetch delay: {str(e)}")
        return False
