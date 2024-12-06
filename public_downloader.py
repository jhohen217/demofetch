import os
import requests
import time
import random
import subprocess
import json
import threading
import asyncio
from urllib.parse import urlparse, parse_qs
from score_filter import start_match_filtering
import sys

# Load configuration
with open('config.json', 'r') as f:
    config = json.load(f)

API_KEY = config['faceit']['api_key']
PROJECT_DIR = config.get('project', {}).get('directory', os.path.dirname(os.path.abspath(__file__)))
BATCH_SIZE = config.get('downloader', {}).get('batch_size', 100)
FETCH_DELAY_MIN = config.get('downloader', {}).get('fetch_delay', {}).get('min', 180)  # 3 minutes default
FETCH_DELAY_MAX = config.get('downloader', {}).get('fetch_delay', {}).get('max', 300)  # 5 minutes default

# Create textfiles directory
TEXTFILES_DIR = os.path.join(PROJECT_DIR, 'textfiles')
os.makedirs(TEXTFILES_DIR, exist_ok=True)

# File paths - moved to textfiles directory
MATCH_IDS_FILE = os.path.join(TEXTFILES_DIR, 'match_ids.txt')
DOWNLOADED_MATCHES_FILE = os.path.join(TEXTFILES_DIR, 'downloaded.txt')
REJECTED_MATCHES_FILE = os.path.join(TEXTFILES_DIR, 'rejected.txt')
DOWNLOAD_QUEUE_FILE = os.path.join(TEXTFILES_DIR, 'download_queue.txt')
ACE_FILE = os.path.join(TEXTFILES_DIR, 'ace_matchids.txt')
QUAD_FILE = os.path.join(TEXTFILES_DIR, 'quad_matchids.txt')
ACEQUAD_FILE = os.path.join(TEXTFILES_DIR, 'acequad_matchids.txt')
UNAPPROVED_FILE = os.path.join(TEXTFILES_DIR, 'unapproved_matchids.txt')

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
    'last_match_id': None
}

def update_progress_bar(match_id, progress):
    """Display a progress bar that morphs into the match ID"""
    bar_length = len(match_id)
    filled_length = int(progress * bar_length)
    
    # Create the progress bar that morphs into the match ID
    bar = ''
    for i in range(bar_length):
        if i < filled_length:
            bar += match_id[i]
        else:
            bar += '█'
    
    # Calculate percentage
    percent = int(progress * 100)
    
    # Create the display string with carriage return and clear to end of line
    status = f'\rDownloading: [{bar}] {percent}%\033[K'
    
    # Print the status
    sys.stdout.write(status)
    sys.stdout.flush()

def fetch_match_ids():
    try:
        print("***Fetching NA East Match IDs***")
        subprocess.run(['python', 'match_scrape.py'], check=True)
        print("Data successfully scraped")
        total_matches = get_match_ids_count()
        print(f"***Found new matches. Total {total_matches} matches in match_ids.txt***")
        return True
    except subprocess.CalledProcessError:
        return False
    except Exception as e:
        print(f"Error running match_scrape.py: {str(e)}")
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

def prepare_auto_download_queue(limit: int = None):
    """
    Prepare download queue in priority order: quadace, ace, quad
    Returns (bool, dict) - success status and stats about the queue preparation
    """
    stats = {
        'total_special': 0,
        'already_downloaded': 0,
        'already_rejected': 0,
        'queued': 0,
        'by_category': {'acequad': 0, 'ace': 0, 'quad': 0}
    }
    
    # Read existing downloaded and rejected matches
    downloaded_matches = set()
    rejected_matches = set()
    
    if os.path.exists(DOWNLOADED_MATCHES_FILE):
        with open(DOWNLOADED_MATCHES_FILE, 'r', encoding='utf-8') as f:
            downloaded_matches = {line.strip() for line in f if line.strip()}
            
    if os.path.exists(REJECTED_MATCHES_FILE):
        with open(REJECTED_MATCHES_FILE, 'r', encoding='utf-8') as f:
            rejected_matches = {line.strip() for line in f if line.strip()}

    print(f"Found {len(downloaded_matches)} downloaded matches and {len(rejected_matches)} rejected matches")

    # Read matches in priority order
    queue_matches = []
    priority_files = [(ACEQUAD_FILE, 'acequad'), (ACE_FILE, 'ace'), (QUAD_FILE, 'quad')]
    
    for file_path, category in priority_files:
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                matches = [line.strip() for line in f if line.strip()]
                # Filter out already processed matches
                new_matches = [m for m in matches if m not in downloaded_matches and m not in rejected_matches]
                stats['by_category'][category] = len(new_matches)
                queue_matches.extend(new_matches)
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
    print(f"  Ace+Quad: {stats['by_category']['acequad']}")
    print(f"  Ace: {stats['by_category']['ace']}")
    print(f"  Quad: {stats['by_category']['quad']}")

    # Write to queue file
    with open(DOWNLOAD_QUEUE_FILE, 'w', encoding='utf-8') as f:
        for match_id in queue_matches:
            f.write(match_id + "\n")

    return bool(queue_matches), stats

def prepare_download_queue(category: str = None, limit: int = None):
    """
    Prepare download queue from specified category or all special matches.
    Returns (bool, dict) - success status and stats about the queue preparation.
    """
    if category == 'auto':
        return prepare_auto_download_queue(limit)
        
    stats = {
        'total_special': 0,
        'already_downloaded': 0,
        'already_rejected': 0,
        'queued': 0
    }
    
    # Read existing downloaded and rejected matches
    downloaded_matches = set()
    rejected_matches = set()
    
    if os.path.exists(DOWNLOADED_MATCHES_FILE):
        with open(DOWNLOADED_MATCHES_FILE, 'r', encoding='utf-8') as f:
            downloaded_matches = {line.strip() for line in f if line.strip()}
            
    if os.path.exists(REJECTED_MATCHES_FILE):
        with open(REJECTED_MATCHES_FILE, 'r', encoding='utf-8') as f:
            rejected_matches = {line.strip() for line in f if line.strip()}

    print(f"Found {len(downloaded_matches)} downloaded matches and {len(rejected_matches)} rejected matches")

    # Read matches from appropriate category file(s)
    queue_matches = set()
    if category:
        category_file = {
            'ace': ACE_FILE,
            'quad': QUAD_FILE,
            'acequad': ACEQUAD_FILE
        }.get(category.lower())
        
        if not category_file:
            return False, {'error': f"Invalid category: {category}"}
            
        if os.path.exists(category_file):
            with open(category_file, 'r', encoding='utf-8') as f:
                matches = {line.strip() for line in f if line.strip()}
                queue_matches.update(matches)
                print(f"Found {len(matches)} matches in {os.path.basename(category_file)}")
    else:
        # Read from all special categories if no specific category
        for file_path in [ACE_FILE, QUAD_FILE, ACEQUAD_FILE]:
            if os.path.exists(file_path):
                with open(file_path, 'r', encoding='utf-8') as f:
                    matches = {line.strip() for line in f if line.strip()}
                    queue_matches.update(matches)
                    print(f"Found {len(matches)} matches in {os.path.basename(file_path)}")

    stats['total_special'] = len(queue_matches)
    
    # Count matches that are already processed
    already_downloaded = queue_matches.intersection(downloaded_matches)
    already_rejected = queue_matches.intersection(rejected_matches)
    stats['already_downloaded'] = len(already_downloaded)
    stats['already_rejected'] = len(already_rejected)

    # Filter out already downloaded and rejected matches
    queue_matches = [mid for mid in queue_matches if mid not in downloaded_matches and mid not in rejected_matches]
    
    # Apply limit if specified
    if limit and limit > 0:
        queue_matches = queue_matches[:limit]
    
    stats['queued'] = len(queue_matches)

    print(f"\nQueue preparation stats:")
    print(f"Total special matches: {stats['total_special']}")
    print(f"Already downloaded: {stats['already_downloaded']}")
    print(f"Already rejected: {stats['already_rejected']}")
    print(f"New matches queued: {stats['queued']}")

    # Write to queue file
    with open(DOWNLOAD_QUEUE_FILE, 'w', encoding='utf-8') as f:
        for match_id in queue_matches:
            f.write(match_id + "\n")

    return bool(queue_matches), stats

def get_category_counts():
    """Get counts for each category of matches"""
    counts = {}
    
    # Count matches in each category file
    for category, file_path in {
        'ace': ACE_FILE,
        'quad': QUAD_FILE,
        'acequad': ACEQUAD_FILE,
        'unapproved': UNAPPROVED_FILE
    }.items():
        if os.path.exists(file_path):
            with open(file_path, 'r', encoding='utf-8') as f:
                counts[category] = sum(1 for line in f if line.strip())
        else:
            counts[category] = 0
            
    return counts

def download_demo(match_id):
    DOWNLOAD_API_URL = 'https://open.faceit.com/download/v2/demos/download'
    DEMO_URL = f'https://demos-us-east.backblaze.faceit-cdn.net/cs2/{match_id}-1-1.dem.gz'
    
    headers = {
        'Authorization': f'Bearer {API_KEY}',
        'Content-Type': 'application/json'
    }
    payload = {
        'resource_url': DEMO_URL
    }
    
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
                    save_path = os.path.join(SAVE_FOLDER, f'{match_id}.dem.gz')
                    total_size = int(demo_response.headers.get('content-length', 0))
                    block_size = 1024  # 1 KB
                    downloaded = 0
                    
                    with open(save_path, 'wb') as demo_file:
                        for chunk in demo_response.iter_content(chunk_size=block_size):
                            demo_file.write(chunk)
                            downloaded += len(chunk)
                            if total_size:
                                progress = downloaded / total_size
                                update_progress_bar(match_id, progress)
                    
                    # Clear the progress bar line
                    sys.stdout.write('\r' + ' ' * (len(match_id) + 20) + '\r')
                    sys.stdout.flush()
                    
                    # Check file size (50,000 KB = 50MB)
                    file_size = os.path.getsize(save_path) / 1024  # Size in KB
                    if file_size < 50000:
                        os.remove(save_path)
                        print(f"Removed {match_id}.dem.gz due to small size ({file_size:.2f} KB)")
                        # Add to rejected.txt
                        with open(REJECTED_MATCHES_FILE, 'a', encoding='utf-8') as f:
                            f.write(match_id + '\n')
                        download_stats['rejected'] += 1
                        return "small_file"
                    
                    # Add match ID to downloaded.txt
                    with open(DOWNLOADED_MATCHES_FILE, 'a', encoding='utf-8') as f:
                        f.write(match_id + '\n')
                    print(f"Successfully downloaded: {match_id}")
                    download_stats['successful'] += 1
                    download_stats['last_match_id'] = match_id
                    return "success"
                else:
                    print(f"\nFailed to download demo file: {demo_response.status_code}")
                    download_stats['failed'] += 1
                    return "failed"
            else:
                print("\nNo signed URL received")
                download_stats['failed'] += 1
                return "no_url"
        else:
            print(f"\nFailed to get signed URL: {response.status_code}")
            with open(REJECTED_MATCHES_FILE, 'a', encoding='utf-8') as f:
                f.write(match_id + '\n')
            download_stats['failed'] += 1
            return "failed"
    except Exception as e:
        print(f"\nError downloading demo: {str(e)}")
        download_stats['failed'] += 1
        return "error"

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
    if os.path.exists(MATCH_IDS_FILE):
        with open(MATCH_IDS_FILE, 'r', encoding='utf-8') as f:
            return sum(1 for line in f if line.strip())
    return 0

def get_downloaded_match_ids_count():
    if os.path.exists(DOWNLOADED_MATCHES_FILE):
        with open(DOWNLOADED_MATCHES_FILE, 'r', encoding='utf-8') as f:
            return sum(1 for line in f if line.strip())
    return 0

def get_rejected_match_ids_count():
    if os.path.exists(REJECTED_MATCHES_FILE):
        with open(REJECTED_MATCHES_FILE, 'r', encoding='utf-8') as f:
            return sum(1 for line in f if line.strip())
    return 0

def get_undownloaded_match_ids_count():
    total_matches = get_match_ids_count()
    downloaded_matches = get_downloaded_match_ids_count()
    rejected_matches = get_rejected_match_ids_count()
    return max(0, total_matches - downloaded_matches - rejected_matches)

async def scraper_loop():
    while not stop_event.is_set():
        if fetch_match_ids():
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

def process_batch(queue):
    """Process a batch of match IDs from the queue"""
    batch = queue[:BATCH_SIZE]
    remaining = queue[BATCH_SIZE:]
    
    for match_id in batch:
        if stop_event.is_set():
            return remaining, True
            
        result = download_demo(match_id)
        
        if result == "rate_limited":
            print("Rate limited. Waiting 2 minutes...")
            time.sleep(120)
            # Add the match back to remaining queue
            remaining = [match_id] + remaining
            return remaining, False
        elif result == "success":
            time.sleep(2)  # Small delay between successful downloads
            
    return remaining, False

def downloader_loop():
    if not os.path.exists(DOWNLOAD_QUEUE_FILE):
        print("No download queue file found")
        return

    with open(DOWNLOAD_QUEUE_FILE, 'r', encoding='utf-8') as f:
        queue = [line.strip() for line in f if line.strip()]

    if not queue:
        print("Download queue is empty")
        return

    # Reset download stats
    download_stats['total'] = len(queue)
    download_stats['successful'] = 0
    download_stats['failed'] = 0
    download_stats['rejected'] = 0
    download_stats['last_match_id'] = None

    while queue and not stop_event.is_set():
        queue, should_stop = process_batch(queue)
        
        # Update queue file with remaining matches
        with open(DOWNLOAD_QUEUE_FILE, 'w', encoding='utf-8') as f:
            for match_id in queue:
                f.write(f"{match_id}\n")
        
        if should_stop:
            break
        
        if not queue:
            print("Download queue is empty")
            break

async def start_match_scraping():
    """Start the match scraping process"""
    stop_event.clear()
    await scraper_loop()

def start_downloading(category: str = None, limit: int = None):
    """Start the demo downloading process"""
    global downloader_thread
    if downloader_thread and downloader_thread.is_alive():
        return "Downloader is already running."
    
    has_matches, stats = prepare_download_queue(category, limit)
    if not has_matches:
        if 'error' in stats:
            return f"Error: {stats['error']}"
        return (
            f"No new matches to download.\n"
            f"Total special matches: {stats['total_special']}\n"
            f"Already downloaded: {stats['already_downloaded']}\n"
            f"Already rejected: {stats['already_rejected']}"
        )
    
    stop_event.clear()
    downloader_thread = threading.Thread(target=downloader_loop, daemon=True)
    downloader_thread.start()
    
    # Add category-specific stats for auto mode
    if category == 'auto' and 'by_category' in stats:
        return (
            f"Demo downloader started.\n"
            f"Total special matches: {stats['total_special']}\n"
            f"Already downloaded: {stats['already_downloaded']}\n"
            f"Already rejected: {stats['already_rejected']}\n"
            f"New matches queued: {stats['queued']}\n"
            f"By category:\n"
            f"  Ace+Quad: {stats['by_category']['acequad']}\n"
            f"  Ace: {stats['by_category']['ace']}\n"
            f"  Quad: {stats['by_category']['quad']}"
        )
    
    return (
        f"Demo downloader started.\n"
        f"Total special matches: {stats['total_special']}\n"
        f"Already downloaded: {stats['already_downloaded']}\n"
        f"Already rejected: {stats['already_rejected']}\n"
        f"New matches queued: {stats['queued']}"
    )

def get_download_stats():
    """Get the current download statistics"""
    return download_stats

def stop_processes():
    """Stop all running processes"""
    stop_event.set()
    if downloader_thread and downloader_thread.is_alive():
        downloader_thread.join()
    return "All processes stopped."
