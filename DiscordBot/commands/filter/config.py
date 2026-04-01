"""
Configuration handling for the filter module.
"""

import os
import configparser
from typing import Dict, List

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
    
    return config

# Valid lowercase month names
_MONTH_NAMES = [
    'january', 'february', 'march', 'april', 'may', 'june',
    'july', 'august', 'september', 'october', 'november', 'december'
]

def get_available_months() -> List[str]:
    """
    Get a list of available month folders based on the textfiles directory structure.
    Supports both the legacy plain-name format (e.g. "February") and the new
    MonthYY format (e.g. "February26").

    Returns:
        List[str]: List of folder names (e.g., ["February26", "March26"])
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
        # Match MonthYY format (e.g. "February26") or legacy plain names
        for m in _MONTH_NAMES:
            if item_lower == m:
                # Legacy plain format
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
        'match_ids_file': os.path.join(month_dir, f"match_ids_{month_lower}.txt"),
        'filter_queue_file': os.path.join(month_dir, f"filter_queue_{month_lower}.txt"),
        'filtered_file': os.path.join(month_dir, f"match_filtered_{month_lower}.txt"),
        'unapproved_file': os.path.join(month_dir, f"unapproved_matchids_{month_lower}.txt"),
        'ace_file': os.path.join(month_dir, f"ace_matchids_{month_lower}.txt"),
        'quad_file': os.path.join(month_dir, f"quad_matchids_{month_lower}.txt"),
        'failed_matches_log': os.path.join(month_dir, f"failed_matches_log_{month_lower}.json"),
        'permanent_fail_file': os.path.join(month_dir, f"permanent_fails_{month_lower}.txt"),
    }
