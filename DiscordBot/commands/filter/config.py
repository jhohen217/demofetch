"""
Configuration handling for the filter module.
"""

import os
import json
from typing import Dict, Any, List, Optional

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
        'match_ids_file': os.path.join(month_dir, f"match_ids_{month_lower}.txt"),
        'filter_queue_file': os.path.join(month_dir, f"filter_queue_{month_lower}.txt"),
        'filtered_file': os.path.join(month_dir, f"match_filtered_{month_lower}.txt"),
        'unapproved_file': os.path.join(month_dir, f"unapproved_matchids_{month_lower}.txt"),
        'ace_file': os.path.join(month_dir, f"ace_matchids_{month_lower}.txt"),
        'quad_file': os.path.join(month_dir, f"quad_matchids_{month_lower}.txt"),
        'failed_matches_log': os.path.join(month_dir, f"failed_matches_log_{month_lower}.json"),
        'permanent_fail_file': os.path.join(month_dir, f"permanent_fails_{month_lower}.txt"),
    }
