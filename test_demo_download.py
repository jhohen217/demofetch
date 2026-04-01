#!/usr/bin/env python3
"""
Test Demo Download Script
Replicates the Discord bot's demo download functionality for testing single demo downloads.
"""

import os
import aiohttp
import asyncio
import configparser
from datetime import datetime

def load_config():
    """Load configuration from config.ini"""
    config = configparser.ConfigParser()
    config_path = 'config.ini'
    
    if not os.path.exists(config_path):
        print(f"❌ Config file not found: {config_path}")
        print("Please create config.ini based on config.ini.example")
        return None
    
    config.read(config_path)
    return config

def strip_match_id_prefix(match_id: str) -> str:
    """Strip the prefix (if any) from a match ID to get the UUID"""
    if '_' in match_id:
        # Format is like: 08-01-25_0100_1-e47a4bcd-83c2-4d51-9cf7-0d52f090302d
        parts = match_id.split('_')
        if len(parts) >= 3:
            return parts[2]  # Return the actual match ID part (including 1- prefix)
    return match_id

def format_match_id(match_id: str) -> str:
    """Format match ID for display, showing date/time prefix and truncated ID"""
    if '_' in match_id:
        # Format is like: 08-01-25_0100_1-e47a4bcd-83c2-4d51-9cf7-0d52f090302d
        parts = match_id.split('_')
        if len(parts) >= 3:
            date_time = f"{parts[0]}_{parts[1]}"  # 08-01-25_0100
            full_id = parts[2]  # 1-e47a4bcd-83c2-4d51-9cf7-0d52f090302d
            # Get the first 8 chars after the "1-" prefix
            short_id = full_id.split('-', 1)[1][:8] if '-' in full_id else full_id[:8]
            return f"{date_time} ({short_id})"
    return match_id

def get_month_from_match_id(match_id: str) -> str:
    """Extract month from match ID date prefix"""
    if '_' in match_id:
        parts = match_id.split('_')
        if len(parts) >= 1:
            date_part = parts[0]  # 08-01-25
            if '-' in date_part:
                date_parts = date_part.split('-')
                if len(date_parts) >= 2:
                    month_num = int(date_parts[0])
                    # Convert month number to month name
                    month_names = [
                        "", "January", "February", "March", "April", "May", "June",
                        "July", "August", "September", "October", "November", "December"
                    ]
                    if 1 <= month_num <= 12:
                        return month_names[month_num]
    return "Unknown"

def get_month_files(month: str, textfiles_dir: str):
    """Get file paths for a specific month"""
    month_dir = os.path.join(textfiles_dir, month)
    month_lower = month.lower()
    return {
        'dir': month_dir,
        'downloaded': os.path.join(month_dir, f'downloaded_{month_lower}.txt'),
        'rejected': os.path.join(month_dir, f'rejected_{month_lower}.txt'),
    }

async def download_demo_async(match_id: str, api_key: str, save_folder: str, textfiles_dir: str) -> str:
    """
    Download a demo file asynchronously and place it in a month-specific folder.
    
    Args:
        match_id: The match ID to download (with or without prefix)
        api_key: FACEIT API key
        save_folder: Base folder for saving demos
        textfiles_dir: Directory for tracking files
        
    Returns:
        str: Status of the download ("success", "rate_limited", "small_file", etc.)
    """
    
    # Determine month from match ID
    month = get_month_from_match_id(match_id)
    print(f"📅 Detected month: {month}")
    
    # Get month-specific file paths
    files = get_month_files(month, textfiles_dir)
    downloaded_file = files['downloaded']
    rejected_file = files['rejected']
    
    # Ensure month directories exist
    os.makedirs(files['dir'], exist_ok=True)
    
    DOWNLOAD_API_URL = 'https://open.faceit.com/download/v2/demos/download'
    # Get base_match_id (UUID part)
    base_match_id = strip_match_id_prefix(match_id)
    print(f"🔑 Extracted UUID: {base_match_id}")
    
    # Try both file extensions (.zst for newer CS2 demos, .gz for older ones)
    url_formats = [
        # CS2 format with .zst extension (newer)
        f'https://demos-us-east.backblaze.faceit-cdn.net/cs2/{base_match_id}-1-1.dem.zst',
        # CS2 format with .gz extension (older)
        f'https://demos-us-east.backblaze.faceit-cdn.net/cs2/{base_match_id}-1-1.dem.gz'
    ]
    
    print(f"🔗 Will try {len(url_formats)} different file extensions (.zst and .gz)...")
    
    headers = {
        'Authorization': f'Bearer {api_key}',
        'Content-Type': 'application/json'
    }

    # Create monthly folder if it doesn't exist
    destination_folder = os.path.join(save_folder, month)
    os.makedirs(destination_folder, exist_ok=True)
    print(f"📁 Destination folder: {destination_folder}")

    try:
        async with aiohttp.ClientSession() as session:
            
            # Try each URL format until we find one that works
            for i, demo_url in enumerate(url_formats, 1):
                print(f"🌐 Trying URL format {i}/{len(url_formats)}: {demo_url}")
                
                payload = {
                    'resource_url': demo_url
                }
                
                # Get signed URL from FACEIT API
                async with session.post(DOWNLOAD_API_URL, headers=headers, json=payload) as response:
                    if response.status == 429:
                        print("⏳ Rate limited by FACEIT API")
                        return "rate_limited"
                    
                    if response.status == 200:
                        response_data = await response.json()
                        signed_url = response_data.get('payload', {}).get('download_url', '')
                        
                        if not signed_url:
                            print(f"❌ No signed URL for format {i}")
                            continue
                        
                        print(f"✅ Got signed URL from format {i}, downloading demo file...")
                        
                        # Download the demo file
                        async with session.get(signed_url) as demo_response:
                            if demo_response.status == 200:
                                # Determine correct file extension based on the URL that worked
                                file_ext = ".dem.zst" if ".zst" in demo_url else ".dem.gz"
                                save_path = os.path.join(destination_folder, f'{base_match_id}{file_ext}')
                                print(f"💾 Saving to: {save_path}")
                                
                                # Get content length for progress tracking
                                content_length = demo_response.headers.get('Content-Length')
                                total_size = int(content_length) if content_length else None
                                downloaded_size = 0
                                
                                # Download the file in chunks
                                with open(save_path, 'wb') as demo_file:
                                    while True:
                                        chunk = await demo_response.content.read(8192)
                                        if not chunk:
                                            break
                                        demo_file.write(chunk)
                                        downloaded_size += len(chunk)
                                        
                                        # Show progress
                                        if total_size:
                                            progress = (downloaded_size / total_size) * 100
                                            print(f"\r📥 Progress: {progress:.1f}% ({downloaded_size:,} / {total_size:,} bytes)", end="", flush=True)
                                        else:
                                            print(f"\r📥 Downloaded: {downloaded_size:,} bytes", end="", flush=True)
                                
                                print()  # New line after progress
                                
                                # Check file size (50,000 KB = 50MB)
                                file_size = os.path.getsize(save_path) / 1024  # Size in KB
                                file_size_mb = file_size / 1024  # Size in MB
                                print(f"📊 File size: {file_size_mb:.2f} MB ({file_size:.0f} KB)")
                                
                                if file_size < 50000:  # Less than 50MB
                                    os.remove(save_path)
                                    print(f"🗑️ File too small ({file_size_mb:.2f} MB), removing and adding to rejected list")
                                    
                                    # Add to month-specific rejected.txt
                                    with open(rejected_file, 'a', encoding='utf-8') as f:
                                        f.write(match_id + '\n')
                                    return "small_file"

                                # Add match ID to month-specific downloaded.txt (without prefix)  
                                base_match_id_save = strip_match_id_prefix(match_id)
                                with open(downloaded_file, 'a', encoding='utf-8') as f:
                                    f.write(base_match_id_save + '\n')
                                
                                print(f"✅ Successfully downloaded demo: {format_match_id(match_id)}")
                                print(f"📝 Added to downloaded list: {downloaded_file}")
                                return "success"
                            else:
                                print(f"❌ HTTP {demo_response.status} for format {i}, trying next format...")
                                continue
                    else:
                        print(f"❌ API error {response.status} for format {i}")
                        response_text = await response.text()
                        print(f"🔍 Response: {response_text}")
                        continue
            
            # If we get here, all URL formats failed
            print(f"❌ All URL formats failed for demo: {format_match_id(match_id)}")
            
            # Add to rejected file
            with open(rejected_file, 'a', encoding='utf-8') as f:
                f.write(match_id + '\n')
            
            return "failed"
    except Exception as e:
        print(f"❌ Failed demo: {format_match_id(match_id)} - {str(e)}")
        return "error"

async def main():
    """Main function to test demo download"""
    print("🎮 Demo Download Test Script")
    print("=" * 40)
    
    # Load configuration
    config = load_config()
    if not config:
        return
    
    # Get configuration values
    try:
        api_key = config.get('Keys', 'faceit_api_key')
        save_folder = config.get('Paths', 'public_demos_directory')
        textfiles_dir = config.get('Paths', 'textfiles_directory')
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        print(f"❌ Configuration error: {e}")
        print("Please check your config.ini file")
        return
    
    # Validate configuration
    if not api_key or api_key == 'YOUR_FACEIT_API_KEY_HERE':
        print("❌ FACEIT API key not configured in config.ini")
        return
    
    # Test match ID (as provided by user)
    test_match_id = "05-03-25_0103_1-c4ba611f-dcb9-499d-8afe-03cae497e5c6"
    
    print(f"🔍 Test Match ID: {test_match_id}")
    print(f"📁 Save Folder: {save_folder}")
    print(f"📂 Textfiles Directory: {textfiles_dir}")
    print()
    
    # Ensure directories exist
    os.makedirs(save_folder, exist_ok=True)
    os.makedirs(textfiles_dir, exist_ok=True)
    
    # Start download
    print("🚀 Starting download...")
    start_time = datetime.now()
    
    result = await download_demo_async(test_match_id, api_key, save_folder, textfiles_dir)
    
    end_time = datetime.now()
    duration = end_time - start_time
    
    print()
    print("=" * 40)
    print(f"⏱️ Download completed in {duration.total_seconds():.2f} seconds")
    print(f"📊 Result: {result}")
    
    if result == "success":
        print("🎉 Demo downloaded successfully!")
    elif result == "small_file":
        print("⚠️ Demo was too small and rejected")
    elif result == "rate_limited":
        print("⏳ Rate limited by API, try again later")
    elif result == "failed":
        print("❌ Download failed")
    else:
        print(f"❓ Unexpected result: {result}")

if __name__ == "__main__":
    # Run the async main function
    asyncio.run(main())
