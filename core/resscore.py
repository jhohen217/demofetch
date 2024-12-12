import os
import glob
from typing import Dict

def calculate_storage_cost() -> tuple:
    """Calculate total storage size, cost, and file count of demos"""
    total_size = 0
    file_count = 0
    cost_per_gb = 0.023  # Example cost per GB

    if os.path.exists("demos"):
        for root, _, files in os.walk("demos"):
            for file in files:
                if file.endswith('.dem'):
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
        with open(os.path.join("textfiles", "match_ids.txt"), 'r') as f:
            return sum(1 for line in f if line.strip())
    except:
        return 0

def get_downloaded_match_ids_count() -> int:
    """Get count of downloaded match IDs"""
    try:
        with open(os.path.join("textfiles", "match_downloaded.txt"), 'r') as f:
            return sum(1 for line in f if line.strip())
    except:
        return 0

def get_rejected_match_ids_count() -> int:
    """Get count of rejected match IDs"""
    try:
        with open(os.path.join("textfiles", "match_rejected.txt"), 'r') as f:
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
    textfiles_dir = "textfiles"

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
