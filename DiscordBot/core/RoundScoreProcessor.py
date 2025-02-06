import os
import glob
import json
from typing import Dict

def calculate_storage_cost() -> tuple:
    """Calculate total storage size, cost, and file count of demos"""
    total_size = 0
    file_count = 0
    cost_per_gb = 0.03  # Cost per GB

    # Load config to get directory paths
    try:
        core_dir = os.path.dirname(os.path.abspath(__file__))  # core directory
        project_dir = os.path.dirname(core_dir)  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(project_dir), 'config.json')
        
        with open(config_path, 'r') as f:
            config = json.load(f)
            project_dir = config['project']['directory']
            public_demos_dir = config['project']['public_demos_directory']
    except Exception as e:
        print(f"Error loading config: {e}")
        return 0, 0, 0

    # Check both demos and public_demos directories
    for directory in [os.path.join(project_dir, "demos"), public_demos_dir]:
        if os.path.exists(directory):
            for root, _, files in os.walk(directory):
                for file in files:
                    # Count both .dem and .dem.gz files
                    if file.endswith(('.dem', '.dem.gz')):
                        file_path = os.path.join(root, file)
                        try:
                            size = os.path.getsize(file_path)
                            total_size += size
                            file_count += 1
                        except OSError:
                            continue

    size_gb = total_size / (1024 * 1024 * 1024)  # Convert to GB
    cost = size_gb * cost_per_gb

    return size_gb, cost, file_count

def _get_month_directories(textfiles_dir: str) -> list:
    """Get list of month directories"""
    return [d for d in os.listdir(textfiles_dir) 
            if os.path.isdir(os.path.join(textfiles_dir, d)) 
            and d not in ['undated', 'MergeMe']]

def _count_lines_in_file(file_path: str) -> int:
    """Count non-empty lines in a file"""
    try:
        with open(file_path, 'r') as f:
            return sum(1 for line in f if line.strip())
    except:
        return 0

def get_match_ids_count() -> int:
    """Get total count of match IDs across all months"""
    try:
        # Load config to get directory paths
        core_dir = os.path.dirname(os.path.abspath(__file__))  # core directory
        project_dir = os.path.dirname(core_dir)  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(project_dir), 'config.json')
        
        with open(config_path, 'r') as f:
            config = json.load(f)
            textfiles_dir = config['project']['textfiles_directory']
        
        total = 0
        # Check each month directory
        for month in _get_month_directories(textfiles_dir):
            month_dir = os.path.join(textfiles_dir, month)
            month_lower = month.lower()
            file_path = os.path.join(month_dir, f"match_ids_{month_lower}.txt")
            total += _count_lines_in_file(file_path)
        
        return total
    except:
        return 0

def get_downloaded_match_ids_count() -> int:
    """Get count of downloaded match IDs across all months"""
    try:
        # Load config to get directory paths
        core_dir = os.path.dirname(os.path.abspath(__file__))  # core directory
        project_dir = os.path.dirname(core_dir)  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(project_dir), 'config.json')
        
        with open(config_path, 'r') as f:
            config = json.load(f)
            textfiles_dir = config['project']['textfiles_directory']
        
        total = 0
        # Check each month directory
        for month in _get_month_directories(textfiles_dir):
            month_dir = os.path.join(textfiles_dir, month)
            month_lower = month.lower()
            file_path = os.path.join(month_dir, f"downloaded_{month_lower}.txt")
            total += _count_lines_in_file(file_path)
        
        return total
    except:
        return 0

def get_rejected_match_ids_count() -> int:
    """Get count of rejected match IDs across all months"""
    try:
        # Load config to get directory paths
        core_dir = os.path.dirname(os.path.abspath(__file__))  # core directory
        project_dir = os.path.dirname(core_dir)  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(project_dir), 'config.json')
        
        with open(config_path, 'r') as f:
            config = json.load(f)
            textfiles_dir = config['project']['textfiles_directory']
        
        total = 0
        # Check each month directory
        for month in _get_month_directories(textfiles_dir):
            month_dir = os.path.join(textfiles_dir, month)
            month_lower = month.lower()
            file_path = os.path.join(month_dir, f"rejected_{month_lower}.txt")
            total += _count_lines_in_file(file_path)
        
        return total
    except:
        return 0

def get_undownloaded_match_ids_count() -> int:
    """Get count of undownloaded match IDs"""
    total = get_match_ids_count()
    downloaded = get_downloaded_match_ids_count()
    rejected = get_rejected_match_ids_count()
    return total - (downloaded + rejected)

def get_category_counts() -> Dict[str, int]:
    """Get counts for each category (ace, quad, unapproved) across all months"""
    counts = {'ace': 0, 'quad': 0, 'unapproved': 0}
    
    # Load config to get directory paths
    core_dir = os.path.dirname(os.path.abspath(__file__))  # core directory
    project_dir = os.path.dirname(core_dir)  # DiscordBot directory
    config_path = os.path.join(os.path.dirname(project_dir), 'config.json')
    
    with open(config_path, 'r') as f:
        config = json.load(f)
        textfiles_dir = config['project']['textfiles_directory']

    # Check each month directory
    for month in _get_month_directories(textfiles_dir):
        month_dir = os.path.join(textfiles_dir, month)
        month_lower = month.lower()
        
        # Count ace matches
        ace_file = os.path.join(month_dir, f"ace_matchids_{month_lower}.txt")
        counts['ace'] += _count_lines_in_file(ace_file)
        
        # Count quad matches
        quad_file = os.path.join(month_dir, f"quad_matchids_{month_lower}.txt")
        counts['quad'] += _count_lines_in_file(quad_file)
        
        # Count unapproved matches
        unapproved_file = os.path.join(month_dir, f"unapproved_matchids_{month_lower}.txt")
        counts['unapproved'] += _count_lines_in_file(unapproved_file)

    return counts
