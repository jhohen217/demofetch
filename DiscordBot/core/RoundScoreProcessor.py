import os
import glob
from typing import Dict

def calculate_storage_cost() -> tuple:
    """Calculate total storage size, cost, and file count of demos"""
    total_size = 0
    file_count = 0
    cost_per_gb = 0.03  # Cost per GB

    # Load config to get directory paths
    try:
        import json
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

def get_match_ids_count() -> int:
    """Get total count of match IDs"""
    try:
        core_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(core_dir)
        textfiles_dir = os.path.join(project_dir, "textfiles")
        with open(os.path.join(textfiles_dir, "match_ids.txt"), 'r') as f:
            return sum(1 for line in f if line.strip())
    except:
        return 0

def get_downloaded_match_ids_count() -> int:
    """Get count of downloaded match IDs"""
    try:
        core_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(core_dir)
        textfiles_dir = os.path.join(project_dir, "textfiles")
        with open(os.path.join(textfiles_dir, "match_downloaded.txt"), 'r') as f:
            return sum(1 for line in f if line.strip())
    except:
        return 0

def get_rejected_match_ids_count() -> int:
    """Get count of rejected match IDs"""
    try:
        core_dir = os.path.dirname(os.path.abspath(__file__))
        project_dir = os.path.dirname(core_dir)
        textfiles_dir = os.path.join(project_dir, "textfiles")
        with open(os.path.join(textfiles_dir, "match_rejected.txt"), 'r') as f:
            return sum(1 for line in f if line.strip())
    except:
        return 0

def get_undownloaded_match_ids_count() -> int:
    """Get count of undownloaded match IDs"""
    total = get_match_ids_count()
    downloaded = get_downloaded_match_ids_count()
    rejected = get_rejected_match_ids_count()
    return total - (downloaded + rejected)

def get_category_counts() -> Dict[str, int]:
    """Get counts for each category (ace, quad, unapproved)"""
    counts = {'ace': 0, 'quad': 0, 'unapproved': 0}
    core_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.dirname(core_dir)
    textfiles_dir = os.path.join(project_dir, "textfiles")

    # Count ace matches
    try:
        with open(os.path.join(textfiles_dir, "ace_matchids.txt"), 'r') as f:
            counts['ace'] = sum(1 for line in f if line.strip())
    except:
        pass

    # Count quad matches
    try:
        with open(os.path.join(textfiles_dir, "quad_matchids.txt"), 'r') as f:
            counts['quad'] = sum(1 for line in f if line.strip())
    except:
        pass

    # Count unapproved matches
    try:
        with open(os.path.join(textfiles_dir, "unapproved_matchids.txt"), 'r') as f:
            counts['unapproved'] = sum(1 for line in f if line.strip())
    except:
        pass

    return counts
