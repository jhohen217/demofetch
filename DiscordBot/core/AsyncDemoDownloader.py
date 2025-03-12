import os
import aiohttp
import asyncio
import json
import logging
import random
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional

logger = logging.getLogger('discord_bot')

# Load configuration from project root
core_dir = os.path.dirname(os.path.abspath(__file__))  # core directory
project_dir = os.path.dirname(core_dir)  # DiscordBot directory
config_path = os.path.join(os.path.dirname(project_dir), 'config.json')

with open(config_path, 'r') as f:
    config = json.load(f)

API_KEY = config['faceit']['api_key']
TEXTFILES_DIR = config['project']['textfiles_directory']
SAVE_FOLDER = config.get('project', {}).get('public_demos_directory', os.path.join(project_dir, 'public_demos'))

# Ensure save folder exists
os.makedirs(SAVE_FOLDER, exist_ok=True)

# Event for controlling the downloader loop
stop_event = asyncio.Event()
downloader_task = None

# Stats for download completion
download_stats = {
    'total': 0,
    'successful': 0,
    'failed': 0,
    'rejected': 0,
    'last_match_id': None,
    'is_complete': False
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
            short_id = full_id.split('-', 1)[1][:8] if '-' in full_id else full_id[:8]
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
                    # Get the UUID part of the match ID
                    match_uuid = strip_match_id_prefix(match)
                    # Check if the UUID is in downloaded_matches or rejected_matches
                    if match_uuid not in downloaded_matches and match_uuid not in rejected_matches and match not in downloaded_matches and match not in rejected_matches:
                        queue_matches.append(match)
                        stats['by_category'][category] = stats['by_category'].get(category, 0) + 1
                if limit and len(queue_matches) >= limit:
                    queue_matches = queue_matches[:limit]
                    break

    stats['total_special'] = sum(stats['by_category'].values())
    stats['already_downloaded'] = len(downloaded_matches)
    stats['already_rejected'] = len(rejected_matches)
    
    # Sort queue_matches alphabetically to ensure chronological order
    # since match IDs start with date/time (e.g., 12-05-24_0101_...)
    queue_matches.sort()
    print("Sorted matches in chronological order")
    
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
                    # Get the UUID part of the match ID
                    match_uuid = strip_match_id_prefix(match)
                    # Check if the UUID is in downloaded_matches or rejected_matches
                    if match_uuid not in downloaded_matches and match_uuid not in rejected_matches and match not in downloaded_matches and match not in rejected_matches:
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
                        # Get the UUID part of the match ID
                        match_uuid = strip_match_id_prefix(match)
                        # Check if the UUID is in downloaded_matches or rejected_matches
                        if match_uuid not in downloaded_matches and match_uuid not in rejected_matches and match not in downloaded_matches and match not in rejected_matches:
                            queue_matches.append(match)
                    print(f"Found {len(matches)} matches in {os.path.basename(file_path)}")

    stats['total_special'] = len(queue_matches)
    stats['already_downloaded'] = len(downloaded_matches)
    stats['already_rejected'] = len(rejected_matches)
    
    # Sort queue_matches alphabetically to ensure chronological order
    # since match IDs start with date/time (e.g., 12-05-24_0101_...)
    queue_matches.sort()
    print("Sorted matches in chronological order")
    
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

async def move_to_broken_matchids(match_id: str, month: str, reason: str = "API error"):
    """
    Move a match ID from ace_matchids and quad_matchids files to broken_matchids file
    
    Args:
        match_id: The match ID to move
        month: Month name (e.g., "February")
        reason: Reason for marking as broken
    """
    # Get month-specific file paths
    files = get_month_files(month)
    
    # Add broken_matchids file to files dict
    month_dir = os.path.join(TEXTFILES_DIR, month)
    month_lower = month.lower()
    broken_file = os.path.join(month_dir, f'broken_matchids_{month_lower}.txt')
    
    # Get the UUID part of the match ID
    match_uuid = strip_match_id_prefix(match_id)
    
    # Create broken_matchids file if it doesn't exist
    if not os.path.exists(broken_file):
        try:
            with open(broken_file, 'w', encoding='utf-8') as f:
                # Add header
                f.write("# Format: match_id|reason|timestamp\n")
        except Exception as e:
            logger.error(f"Error creating broken_matchids file: {str(e)}")
    
    # Add the match ID to broken_matchids file with reason and timestamp
    try:
        with open(broken_file, 'a', encoding='utf-8') as f:
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            f.write(f"{match_id}|{reason}|{timestamp}\n")
        print(f"Added failed demo to broken_matchids_{month_lower}.txt: {match_id}")
    except Exception as e:
        logger.error(f"Error adding match ID to broken_matchids file: {str(e)}")
    
    # Check both ace and quad files
    for file_path in [files['ace'], files['quad']]:
        if os.path.exists(file_path):
            try:
                # Read the file
                with open(file_path, 'r', encoding='utf-8') as f:
                    lines = f.readlines()
                
                # Filter out the match ID
                filtered_lines = []
                removed = False
                for line in lines:
                    line_uuid = strip_match_id_prefix(line.strip())
                    if line_uuid != match_uuid:
                        filtered_lines.append(line)
                    else:
                        removed = True
                
                # Write back if any were removed
                if removed:
                    with open(file_path, 'w', encoding='utf-8') as f:
                        f.writelines(filtered_lines)
                    print(f"Removed failed demo from {os.path.basename(file_path)}")
            except Exception as e:
                logger.error(f"Error removing match ID from {os.path.basename(file_path)}: {str(e)}")

async def download_demo_async(match_id: str, month: str) -> str:
    """
    Download a demo file asynchronously and place it in a month-specific folder.
    
    Args:
        match_id: The match ID to download.
        month: Month name (e.g., "February") for file organization.
        
    Returns:
        str: Status of the download ("success", "rate_limited", "small_file", etc.)
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
        async with aiohttp.ClientSession() as session:
            # Get signed URL from FACEIT API
            async with session.post(DOWNLOAD_API_URL, headers=headers, json=payload) as response:
                if response.status == 429:
                    print("\nRate limited by FACEIT API")
                    download_stats['failed'] += 1
                    return "rate_limited"
                
                if response.status == 200:
                    response_data = await response.json()
                    signed_url = response_data.get('payload', {}).get('download_url', '')
                    
                    if not signed_url:
                        print(f"[✗] Failed demo: {format_match_id(match_id)} - No signed URL")
                        download_stats['failed'] += 1
                        return "no_url"
                    
                    # Download the demo file
                    async with session.get(signed_url) as demo_response:
                        if demo_response.status == 200:
                            save_path = os.path.join(destination_folder, f'{base_match_id}.dem.gz')
                            
                            # Download the file in chunks
                            with open(save_path, 'wb') as demo_file:
                                while True:
                                    chunk = await demo_response.content.read(8192)
                                    if not chunk:
                                        break
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

                            # Add match ID to month-specific downloaded.txt (without prefix)
                            base_match_id = strip_match_id_prefix(match_id)
                            with open(downloaded_file, 'a', encoding='utf-8') as f:
                                f.write(base_match_id + '\n')
                            print(f"[✓] Downloaded demo: {format_match_id(match_id)}")
                            download_stats['successful'] += 1
                            download_stats['last_match_id'] = match_id
                            return "success"
                        else:
                            print(f"[✗] Failed demo: {format_match_id(match_id)} - HTTP {demo_response.status}")
                            download_stats['failed'] += 1
                            return "failed"
                else:
                    print(f"[✗] Failed demo: {format_match_id(match_id)} - API error {response.status}")
                    
                    # Add to rejected file
                    with open(rejected_file, 'a', encoding='utf-8') as f:
                        f.write(match_id + '\n')
                    
                    # Move to broken_matchids file
                    await move_to_broken_matchids(match_id, month, f"API error {response.status}")
                    
                    download_stats['failed'] += 1
                    return "failed"
    except Exception as e:
        print(f"[✗] Failed demo: {format_match_id(match_id)} - {str(e)}")
        download_stats['failed'] += 1
        return "error"

async def process_downloads_async(match_ids: list, month: str) -> str:
    """Process the actual downloads after queue preparation using async downloads"""
    successful = 0
    failed = 0
    remaining_ids = match_ids.copy()  # Copy the list to track remaining IDs
    
    # Get month-specific file paths
    files = get_month_files(month)
    queue_file = files['download_queue']
    
    # Create a semaphore to limit concurrent downloads
    # This prevents overwhelming the API or network
    semaphore = asyncio.Semaphore(5)  # Allow 5 concurrent downloads
    
    async def download_with_semaphore(match_id):
        async with semaphore:
            result = await download_demo_async(match_id, month)
            
            # Update the queue file after each download
            if result == "success" or result == "small_file":
                # Remove from remaining IDs
                if match_id in remaining_ids:
                    remaining_ids.remove(match_id)
                
                # Update the queue file
                try:
                    # Write the updated queue back to the file
                    with open(queue_file, 'w', encoding='utf-8') as f:
                        for id in remaining_ids:
                            f.write(f"{id}\n")
                except Exception as e:
                    logger.error(f"Error updating queue file: {str(e)}")
            
            return result
    
    # Process downloads in batches to allow for queue updates
    batch_size = 10  # Process 10 at a time
    for i in range(0, len(match_ids), batch_size):
        batch = match_ids[i:i+batch_size]
        
        # Create tasks for this batch
        tasks = [download_with_semaphore(match_id) for match_id in batch]
        
        # Process this batch concurrently
        batch_results = await asyncio.gather(*tasks)
        
        # Count successes and failures
        for result in batch_results:
            if result == "success":
                successful += 1
            else:
                failed += 1
    
    download_stats['is_complete'] = True
    completion_message = f"Download complete! Successfully downloaded {successful} matches, {failed} failed."
    print(completion_message)
    
    # Final update to the queue file to ensure it's empty if all downloads were successful
    if not remaining_ids:
        try:
            with open(queue_file, 'w', encoding='utf-8') as f:
                pass  # Write an empty file
        except Exception as e:
            logger.error(f"Error clearing queue file: {str(e)}")
    
    return completion_message

async def start_downloading_async(category: str, month: str, limit: int = None) -> tuple[str, list]:
    """
    Start downloading demos from the specified category and month using async downloads
    
    Args:
        category: 'ace' or 'quad'
        month: Month name (e.g., "February")
        limit: Maximum number of matches to download, or None for all
        
    Returns:
        tuple: (initial message, match_ids list for processing)
    """
    # Reset the stop event to allow new downloads to start
    reset_stop_event()
    
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
    if downloader_task and not downloader_task.done():
        # We can't join an asyncio task like a thread, but we can set the stop event
        # and the task should check it and exit gracefully
        print("Stop event set, waiting for downloader task to complete...")
    return True

def reset_stop_event():
    """Reset the stop event to allow new downloads to start"""
    stop_event.clear()
    print("Download stop event has been reset, ready for new downloads")
    return True

async def scraper_loop():
    """Scraper loop for fetching match IDs and filtering them"""
    from .FaceitMatchScraper import start_match_scraping as start_match_scraping_core
    from .MatchScoreFilter import start_match_filtering
    
    FETCH_DELAY_MIN = config.get('downloader', {}).get('fetch_delay', {}).get('min', 180)  # 3 minutes default
    FETCH_DELAY_MAX = config.get('downloader', {}).get('fetch_delay', {}).get('max', 300)  # 5 minutes default
    
    while not stop_event.is_set():
        try:
            print("***Fetching NA East Match IDs***")
            result = await start_match_scraping_core()
            if result:
                print("Data successfully scraped")
                # After successfully fetching new matches, run the score filter
                await start_match_filtering()
            else:
                print("Failed to fetch match data")
        except Exception as e:
            print(f"Error during match scraping: {str(e)}")
        
        # Wait between min and max delay time
        wait_time = random.randint(FETCH_DELAY_MIN, FETCH_DELAY_MAX)
        print(f"Next fetch in {wait_time} seconds...")
        for _ in range(wait_time):
            if stop_event.is_set():
                return
            await asyncio.sleep(1)
