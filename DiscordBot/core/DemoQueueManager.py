import os
from typing import List

def mark_as_parsed(parsed_txt_path: str, match_id: str):
    """Mark a match as parsed, avoiding duplicates"""
    # Read existing parsed matches
    parsed_matches = set()
    if os.path.exists(parsed_txt_path):
        with open(parsed_txt_path, 'r', encoding='utf-8') as f:
            parsed_matches = {line.strip() for line in f if line.strip()}
    
    # Only append if not already parsed
    if match_id not in parsed_matches:
        with open(parsed_txt_path, 'a', encoding='utf-8') as f:
            f.write(match_id + "\n")

def read_queue(queue_path: str) -> List[str]:
    """Read match IDs from queue"""
    if not os.path.exists(queue_path):
        return []
    with open(queue_path, 'r', encoding='utf-8') as f:
        return [line.strip() for line in f if line.strip()]

def write_queue(queue_path: str, match_ids: List[str]):
    """Write match IDs to queue"""
    with open(queue_path, 'w', encoding='utf-8') as f:
        for match_id in match_ids:
            f.write(f"{match_id}\n")

def add_to_queue(queue_path: str, match_ids: List[str], parsed_txt_path: str) -> int:
    """Add matches to the parsing queue if not already parsed"""
    # Read existing parsed matches and current queue
    parsed_matches = set()
    if os.path.exists(parsed_txt_path):
        with open(parsed_txt_path, 'r', encoding='utf-8') as f:
            parsed_matches = {line.strip() for line in f if line.strip()}
    
    current_queue = read_queue(queue_path)
    current_queue_set = set(current_queue)
    
    added_count = 0
    for match_id in match_ids:
        if match_id not in parsed_matches and match_id not in current_queue_set:
            current_queue.append(match_id)
            added_count += 1
    
    if added_count > 0:
        write_queue(queue_path, current_queue)
    
    print(f"Added {added_count} matches to queue")
    print("--- EVENT PARSING ---")
    return added_count
