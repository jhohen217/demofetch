import os
import requests
import time
import random
import subprocess
import json
import threading
import asyncio
from urllib.parse import urlparse, parse_qs
from datetime import datetime
from .MatchScoreFilter import start_match_filtering
from .FaceitMatchScraper import start_match_scraping as start_match_scraping_core
import sys
import logging

__version__ = "1.0.2"  # Enhanced error handling and retry system

# Configure logging to suppress debug messages
logging.getLogger("urllib3").setLevel(logging.WARNING)

# Load configuration from project root
core_dir = os.path.dirname(os.path.abspath(__file__))  # core directory
project_dir = os.path.dirname(core_dir)  # DiscordBot directory
config_path = os.path.join(os.path.dirname(project_dir), 'config.json')

with open(config_path, 'r') as f:
    config = json.load(f)

API_KEY = config['faceit']['api_key']
PROJECT_DIR = config.get('project', {}).get('directory', os.path.dirname(os.path.abspath(__file__)))
BATCH_SIZE = config.get('downloader', {}).get('batch_size', 100)
FETCH_DELAY_MIN = config.get('downloader', {}).get('fetch_delay', {}).get('min', 180)  # 3 minutes default
FETCH_DELAY_MAX = config.get('downloader', {}).get('fetch_delay', {}).get('max', 300)  # 5 minutes default

# Use configured textfiles directory
TEXTFILES_DIR = config['project']['textfiles_directory']
CURRENT_MONTH = datetime.now().strftime("%B")  # e.g., "February"
MONTH_DIR = os.path.join(TEXTFILES_DIR, CURRENT_MONTH)
os.makedirs(MONTH_DIR, exist_ok=True)

# Use new public_demos_directory config with fallback to old location
SAVE_FOLDER = config.get('project', {}).get('public_demos_directory', os.path.join(PROJECT_DIR, 'public_demos'))

# Ensure save folder exists
os.makedirs(SAVE_FOLDER, exist_ok=True)

# Event for controlling the downloader loop
stop_event = threading.Event()
downloader_thread = None

# Stats for download completion
download_stats = {
    'total': 0,
    'successful': 0,
    'failed': 0,
    'rejected': 0,
    'last_match_id': None,
    'is_complete': False  # New flag to track completion
}

def format_match_id(match_id: str) -> str:
    """Format match ID for display, showing date/time prefix and truncated ID"""
    if '_' in match_id:
        # Format is like: 12-05-24_0101_1-72d8eb18-0bc8-49af-a738-55b792248b79
        parts = match_id.split('_')
        if len(parts) >= 3:
            date_time = f"{parts[0]}_{parts[1]}"  # 12-05-24_0101
            full_id = parts[2]  # 1-72d8eb18-0bc8-49af-a738-55b792248b79
            # Get the first 8 chars after the "1-" prefix
            short_id = full_id.split('-', 1)[1][:8]
            return f"{date_time} ({short_id})"
    return match_id

def strip_match_id_prefix(match_id: str) -> str:
    """Strip the prefix (if any) from a match ID"""
    if '_' in match_id:
        # Format is like: 12-05-24_0101_1-72d8eb18-0bc8-49af-a738-55b792248b79
        parts = match_id.split('_')
        if len(parts) >= 3:
            return parts[2]  # Return the actual match ID part
    return match_id

def download_demo(match_id, month: str):
    """
    Download a demo file and place it in a month-specific folder.
    Args:
        match_id: The match ID to download.
        month: Month name (e.g., "February") for file organization.
    """
    # Get month-specific file paths
    files = get_month_files(month)
    downloaded_file = files['downloaded']
    rejected_file = files['rejected']
    
    DOWNLOAD_API_URL = 'https://open.faceit.com/download/v2/demos/download'
    # Get base_match_id 
    base_match_id = strip_match_id_prefix(match_id)
    DEMO_URL = f'https://demos-us-east.backblaze.faceit-cdn.net/cs2/{base_match_id}-1-1.dem.gz'
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'Content-Type': 'application/json'
    }

    payload = {
        'resource_url': DEMO_URL
    }

    # Create monthly folder if it doesn't exist
    destination_folder = os.path.join(SAVE_FOLDER, month)
    os.makedirs(destination_folder, exist_ok=True)

    try:
        response = requests.post(DOWNLOAD_API_URL, headers=headers, json=payload)

        if response.status_code == 429:
            print("\nRate limited by FACEIT API")
            download_stats['failed'] += 1
            return "rate_limited"
        if response.status_code == 200:
            signed_url = response.json().get('payload', {}).get('download_url', '')
            if signed_url:
                demo_response = requests.get(signed_url, stream=True)
                if demo_response.status_code == 200:
                    save_path = os.path.join(destination_folder, f'{base_match_id}.dem.gz')
                    with open(save_path, 'wb') as demo_file:
                        for chunk in demo_response.iter_content(chunk_size=8192):
                            if chunk:  # filter out keep-alive chunks
                                demo_file.write(chunk)

                    # Check file size (50,000 KB = 50MB)
                    file_size = os.path.getsize(save_path) / 1024
                    if file_size < 50000:
                        os.remove(save_path)
                        # Add to month-specific rejected.txt
                        with open(rejected_file, 'a', encoding='utf-8') as f:
                            f.write(match_id + '\n')
                        download_stats['rejected'] += 1
                        return "small_file"

                    # Add match ID to month-specific downloaded.txt
                    with open(downloaded_file, 'a', encoding='utf-8') as f:
                        f.write(match_id + '\n')
                    print(f"[✓] Downloaded demo: {format_match_id(match_id)}")
                    download_stats['successful'] += 1
                    download_stats['last_match_id'] = match_id
                    return "success"
                else:
                    print(f"[✗] Failed demo: {format_match_id(match_id)} - HTTP {demo_response.status_code}")
                    download_stats['failed'] += 1
                    return "failed"
            else:
                print(f"[✗] Failed demo: {format_match_id(match_id)} - No signed URL")
                download_stats['failed'] += 1
                return "no_url"
        else:
            print(f"[✗] Failed demo: {format_match_id(match_id)} - API error {response.status_code}")
            with open(rejected_file, 'a', encoding='utf-8') as f:
                f.write(match_id + '\n')
            download_stats['failed'] += 1
            return "failed"
    except Exception as e:
        print(f"[✗] Failed demo: {format_match_id(match_id)} - {str(e)}")
        download_stats['failed'] += 1
        return "error"

async def fetch_match_ids():
    """Fetch match IDs using the core function"""
    try:
        print("***Fetching NA East Match IDs***")
        result = await start_match_scraping_core()
        if result:
            print("Data successfully scraped")
            total_matches = get_match_ids_count()
            print(f"***Found new matches. Total {total_matches} matches in match_ids.txt***")
            return True
        else:
            print("Failed to fetch match data")
            return False
    except Exception as e:
        print(f"Error during match scraping: {str(e)}")
        return False

async def filter_matches():
    """Run score filtering on matches"""
    print("\nStarting match filtering...")
    try:
        result = await start_match_filtering()
        if result:
            print("Match filtering completed successfully")
            return True
        else:
            print("Match filtering encountered an error")
            return False
    except Exception as e:
        print(f"Error during match filtering: {str(e)}")
        return False

def get_month_files(month: str):
    """Get file paths for a specific month"""
    month_dir = os.path.join(TEXTFILES_DIR, month)
    month_lower = month.lower()
    return {
        'dir': month_dir,
        'match_ids': os.path.join(month_dir, f'match_ids_{month_lower}.txt'),
        'downloaded': os.path.join(month_dir, f'downloaded_{month_lower}.txt'),
        'rejected': os.path.join(month_dir, f'rejected_{month_lower}.txt'),
        'download_queue': os.path.join(month_dir, f'download_queue_{month_lower}.txt'),
        'ace': os.path.join(month_dir, f'ace_matchids_{month_lower}.txt'),
        'quad': os.path.join(month_dir, f'quad_matchids_{month_lower}.txt'),
        'unapproved': os.path.join(month_dir, f'unapproved_matchids_{month_lower}.txt')
    }

def prepare_auto_download_queue(month: str, limit: int = None):
    """
    Prepare download queue in priority order: ace, quad
    Args:
        month: Month name (e.g., "February")
        limit: Maximum number of matches to queue, or None for all
    Returns (bool, dict) - success status and stats about the queue preparation
    """
    # Get month-specific file paths
    files = get_month_files(month)
    if not os.path.exists(files['dir']):
        return False, {'error': f"No data found for month: {month}"}
    stats = {
        'total_special': 0,
        'already_downloaded': 0,
        'already_rejected': 0,
        'queued': 0,
        'by_category': {'ace': 0, 'quad': 0}
    }
    
    # Get month-specific file paths
    downloaded_file = files['downloaded']
    rejected_file = files['rejected']
    ace_file = files['ace']
    quad_file = files['quad']
    queue_file = files['download_queue']

    # Read existing downloaded and rejected matches
    downloaded_matches = set()
    rejected_matches = set()
    
    if os.path.exists(downloaded_file):
        with open(downloaded_file, 'r', encoding='utf-8') as f:
            downloaded_matches = {line.strip() for line in f if line.strip()}
            
    if os.path.exists(rejected_file):
        with open(rejected_file, 'r', encoding='utf-8') as f:
            rejected_matches = {line.strip() for line in f if line.strip()}

    print(f"Found {len(downloaded_matches)} downloaded matches and {len(rejected_matches)} rejected matches")

    # Read matches in priority order
    queue_matches = []
    priority_files = [(files['ace'], 'ace'), (files['quad'], 'quad')]
    
    for file_path, category in priority_files:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                matches = [line.strip() for line in f if line.strip()]
                # Only add matches that haven't been processed yet, maintaining order
                for match in matches:
                    if match not in downloaded_matches and match not in rejected_matches:
                        queue_matches.append(match)
                        stats['by_category'][category] = stats['by_category'].get(category, 0) + 1
                if limit and len(queue_matches) >= limit:
                    queue_matches = queue_matches[:limit]
                    break

    stats['total_special'] = sum(stats['by_category'].values())
    stats['already_downloaded'] = len(downloaded_matches)
    stats['already_rejected'] = len(rejected_matches)
    stats['queued'] = len(queue_matches)

    print(f"\nQueue preparation stats:")
    print(f"Total special matches: {stats['total_special']}")
    print(f"Already downloaded: {stats['already_downloaded']}")
    print(f"Already rejected: {stats['already_rejected']}")
    print(f"New matches queued: {stats['queued']}")
    print(f"By category:")
    print(f"  Ace: {stats['by_category']['ace']}")
    print(f"  Quad: {stats['by_category']['quad']}")

    # Write to queue file
    with open(queue_file, 'w', encoding='utf-8') as f:
        for match_id in queue_matches:
            f.write(match_id + "\n")

    return bool(queue_matches), stats

def prepare_download_queue(category: str = None, month: str = None, limit: int = None):
    """
    Prepare download queue from specified category or all special matches.
    Args:
        category: 'ace' or 'quad'
        month: Month name (e.g., "February")
        limit: Maximum number of matches to queue, or None for all
    Returns (bool, dict) - success status and stats about the queue preparation.
    """
    if category == 'auto':
        return prepare_auto_download_queue(month, limit)

    # Get month directory path
    month_dir = os.path.join(TEXTFILES_DIR, month)
    if not os.path.exists(month_dir):
        return False, {'error': f"No data found for month: {month}"}
        
    stats = {
        'total_special': 0,
        'already_downloaded': 0,
        'already_rejected': 0,
        'queued': 0
    }
    
    # Get month-specific file paths
    files = get_month_files(month)
    
    # Read existing downloaded and rejected matches
    downloaded_matches = set()
    rejected_matches = set()
    
    if os.path.exists(files['downloaded']):
        with open(files['downloaded'], 'r', encoding='utf-8') as f:
            downloaded_matches = {line.strip() for line in f if line.strip()}
            
    if os.path.exists(files['rejected']):
        with open(files['rejected'], 'r', encoding='utf-8') as f:
            rejected_matches = {line.strip() for line in f if line.strip()}

    print(f"Found {len(downloaded_matches)} downloaded matches and {len(rejected_matches)} rejected matches")

    # Get month-specific file paths
    files = get_month_files(month)
    
    # Read matches from appropriate category file(s)
    queue_matches = []
    if category:
        category_file = {
            'ace': files['ace'],
            'quad': files['quad']
        }.get(category.lower())
        
        if not category_file:
            return False, {'error': f"Invalid category: {category}"}
            
        if os.path.exists(category_file):
            with open(category_file, 'r', encoding='utf-8') as f:
                matches = [line.strip() for line in f if line.strip()]
                # Only add matches that haven't been processed yet
                for match in matches:
                    if match not in downloaded_matches and match not in rejected_matches:
                        queue_matches.append(match)
                print(f"Found {len(matches)} matches in {os.path.basename(category_file)}")
    else:
        # Read from all special categories if no specific category
        for file_path in [files['ace'], files['quad']]:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    matches = [line.strip() for line in f if line.strip()]
                    # Only add matches that haven't been processed yet
                    for match in matches:
                        if match not in downloaded_matches and match not in rejected_matches:
                            queue_matches.append(match)
                    print(f"Found {len(matches)} matches in {os.path.basename(file_path)}")

    stats['total_special'] = len(queue_matches)
    stats['already_downloaded'] = len(downloaded_matches)
    stats['already_rejected'] = len(rejected_matches)
    
    # Apply limit if specified
    if limit and limit > 0:
        queue_matches = queue_matches[:limit]
    
    stats['queued'] = len(queue_matches)

    print(f"\nQueue preparation stats:")
    print(f"Total special matches: {stats['total_special']}")
    print(f"Already downloaded: {stats['already_downloaded']}")
    print(f"Already rejected: {stats['already_rejected']}")
    print(f"New matches queued: {stats['queued']}")

    # Write to month-specific queue file
    with open(files['download_queue'], 'w', encoding='utf-8') as f:
        for match_id in queue_matches:
            f.write(match_id + "\n")

    return bool(queue_matches), stats

def get_category_counts():
    """Get counts for each category of matches"""
    counts = {}
    
    # Get current month's files
    files = get_month_files(CURRENT_MONTH)
    
    # Count matches in each category file
    for category, file_path in {
        'ace': files['ace'],
        'quad': files['quad'],
        'unapproved': files['unapproved']
    }.items():
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                counts[category] = sum(1 for line in f if line.strip())
        else:
            counts[category] = 0
            
    return counts

def calculate_storage_cost():
    total_size = 0
    file_count = 0
    for root, dirs, files in os.walk(SAVE_FOLDER):
        for file in files:
            if file.endswith('.dem.gz') or file.endswith('.dem'):
                file_path = os.path.join(root, file)
                total_size += os.path.getsize(file_path)
                file_count += 1
    
    # Convert to GB and calculate cost ($0.03 per GB)
    size_gb = total_size / (1024 * 1024 * 1024)
    cost = size_gb * 0.03
    
    return size_gb, cost, file_count

def get_match_ids_count():
    files = get_month_files(CURRENT_MONTH)
    if os.path.exists(files['match_ids']):
        with open(files['match_ids'], 'r', encoding='utf-8') as f:
            return sum(1 for line in f if line.strip())
    return 0

def get_downloaded_match_ids_count():
    files = get_month_files(CURRENT_MONTH)
    if os.path.exists(files['downloaded']):
        with open(files['downloaded'], 'r', encoding='utf-8') as f:
            return sum(1 for line in f if line.strip())
    return 0

def get_rejected_match_ids_count():
    files = get_month_files(CURRENT_MONTH)
    if os.path.exists(files['rejected']):
        with open(files['rejected'], 'r', encoding='utf-8') as f:
            return sum(1 for line in f if line.strip())
    return 0

def get_undownloaded_match_ids_count():
    total_matches = get_match_ids_count()
    downloaded_matches = get_downloaded_match_ids_count()
    rejected_matches = get_rejected_match_ids_count()
    return max(0, total_matches - downloaded_matches - rejected_matches)

async def scraper_loop():
    while not stop_event.is_set():
        result = await fetch_match_ids()
        if result:
            # After successfully fetching new matches, run the score filter
            await filter_matches()
        else:
            print("Failed to fetch match IDs")
        
        # Wait between min and max delay time
        wait_time = random.randint(FETCH_DELAY_MIN, FETCH_DELAY_MAX)
        print(f"Next fetch in {wait_time} seconds...")
        for _ in range(wait_time):
            if stop_event.is_set():
                return
            await asyncio.sleep(1)

async def process_downloads(match_ids: list, month: str) -> str:
    """Process the actual downloads after queue preparation"""
    successful = 0
    failed = 0
    
    # Download each match
    for match_id in match_ids:
        if stop_event.is_set():
            break
        result = download_demo(match_id, month)
        if result == "success":
            successful += 1
        else:
            failed += 1
            
    download_stats['is_complete'] = True
    completion_message = f"Download complete! Successfully downloaded {successful} matches, {failed} failed."
    print(completion_message)
    return completion_message

async def start_downloading(category: str, month: str, limit: int = None) -> tuple[str, list]:
    """
    Start downloading demos from the specified category and month
    Args:
        category: 'ace' or 'quad'
        month: Month name (e.g., "February")
        limit: Maximum number of matches to download, or None for all
    Returns:
        tuple: (initial message, match_ids list for processing)
    """
    # Reset download stats
    download_stats['total'] = 0
    download_stats['successful'] = 0
    download_stats['failed'] = 0
    download_stats['rejected'] = 0
    download_stats['last_match_id'] = None
    download_stats['is_complete'] = False

    # Prepare download queue
    success, stats = prepare_download_queue(category, month, limit)
    if not success:
        if 'error' in stats:
            return (f"Error: {stats['error']}", [])
        return ("No matches to download", [])

    # Get month-specific file paths
    files = get_month_files(month)

    # Read the queue file
    if not os.path.exists(files['download_queue']):
        return ("No matches in queue", [])

    with open(files['download_queue'], 'r', encoding='utf-8') as f:
        match_ids = [line.strip() for line in f if line.strip()]

    if not match_ids:
        return ("No matches in queue", [])

    download_stats['total'] = len(match_ids)
    start_message = f"Starting download of {len(match_ids)} matches from {category} category..."
    
    return (start_message, match_ids)

def get_download_stats():
    """Get current download statistics"""
    return download_stats

def stop_processes():
    """Stop all running processes"""
    stop_event.set()
    if downloader_thread and downloader_thread.is_alive():
        downloader_thread.join()
    return True
