import asyncio
import logging
import os
import time
from datetime import datetime
from core.FaceitUserFetcher import fetch_user_matches
from core.UserDemoDownloader import download_user_demos
from core.AsyncDemoDownloader import (
    start_downloading_async,
    process_downloads_async,
    get_download_stats,
    stop_event as download_stop_event
)
from commands.sort_matchids import sort_all_matchid_files
from commands.parser.service import parser_stats

def validate_month(month: str) -> str:
    """
    Validate and format month name.
    Returns formatted month name or None if invalid.
    """
    try:
        # Convert to datetime to validate month name
        month_num = datetime.strptime(month, "%B").month
        # Convert back to month name to ensure consistent capitalization
        return datetime(2000, month_num, 1).strftime("%B")
    except ValueError:
        return None

logger = logging.getLogger('discord_bot')

class DownloadState:
    def __init__(self):
        self.download_task = None

download_state = DownloadState()

async def handle_message(bot, message):
    """Handle message-based download commands"""
    content = message.content.lower()
    args = content.split()
    command = args[0] if args else ""

    if command == 'sortids':
        # Get month if provided
        month = args[1].capitalize() if len(args) > 1 else None
        
        if month and not validate_month(month):
            await bot.send_message(message.author, "Invalid month name. Please use full month name (e.g., February).")
            return True
            
        # Load configuration from project root
        core_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # core directory
        project_dir = os.path.dirname(core_dir)  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(project_dir), 'config.json')
        
        import json
        with open(config_path, 'r') as f:
            config = json.load(f)
            
        textfiles_dir = config['project']['textfiles_directory']
        
        # Sort matchid files
        results = sort_all_matchid_files(textfiles_dir, month)
        
        # Send results
        if month:
            await bot.send_message(message.author, f"Sorted matchid files for {month}: {results['success']} successful, {results['failed']} failed, {results['skipped']} skipped")
        else:
            await bot.send_message(message.author, f"Sorted all matchid files: {results['success']} successful, {results['failed']} failed, {results['skipped']} skipped")
        return True
        
    elif command == 'download':
        # Check if we have the 'loop' option
        loop_mode = False
        if len(args) >= 5 and args[4].lower() == 'loop':
            loop_mode = True
            if len(args) != 5:
                await bot.send_message(message.author, "Usage: download <category> <month> <number> [loop]\nExample: download ace february 100 loop")
                return True
        elif len(args) != 4:
            await bot.send_message(message.author, "Usage: download <category> <month> <number> [loop]\nExample: download ace february 100")
            return True
            
        category, month, number = args[1:4]
        
        # Validate category
        if category.lower() not in ['ace', 'quad']:
            await bot.send_message(message.author, "Invalid category. Please use 'ace' or 'quad'.")
            return True
            
        # Validate month
        formatted_month = validate_month(month.capitalize())
        if not formatted_month:
            await bot.send_message(message.author, "Invalid month name. Please use full month name (e.g., February).")
            return True
            
        # Validate number
        try:
            limit = None if number.lower() == 'all' else int(number)
            if limit is not None and limit <= 0:
                await bot.send_message(message.author, "Number must be positive or 'all'.")
                return True
        except ValueError:
            await bot.send_message(message.author, "Invalid number. Please use a positive number or 'all'.")
            return True
            
        if download_state.download_task and not download_state.download_task.done():
            await bot.send_message(message.author, "Download already in progress")
            return True
            
        try:
            # If loop mode is enabled, create a continuous download task
            if loop_mode:
                # Create task for continuous downloading
                download_state.download_task = asyncio.create_task(
                    continuous_download_loop(category.lower(), formatted_month, limit, bot, message.author)
                )
                
                # Inform user about continuous download mode
                await bot.send_message(message.author, 
                    f"Starting continuous download of {category.lower()} demos for {formatted_month} in batches of {limit if limit else 'all'} demos.\n"
                    f"Will wait for parser to finish processing before downloading next batch."
                )
                
                return True
            
            # Regular (non-loop) download mode
            # Reset the stop event to allow new downloads to start (this is also done in start_downloading_async, but doing it here for clarity)
            from core.AsyncDemoDownloader import reset_stop_event
            reset_stop_event()
            
            # Get initial message and match list
            start_message, match_ids = await start_downloading_async(category.lower(), formatted_month, limit)
            
            # Calculate estimated size and cost
            if match_ids:
                # Estimate 257MB per demo (based on info command)
                estimated_size_gb = len(match_ids) * 257 / 1024
                # Cost is $0.03 per GB
                estimated_cost = estimated_size_gb * 0.03
                start_message += f"\nEstimated size: {estimated_size_gb:.2f} GB"
                start_message += f"\nEstimated cost: ${estimated_cost:.2f}"
            
            await bot.send_message(message.author, start_message)
            
            if match_ids:  # Only process if we have matches
                # Create task for processing downloads
                download_state.download_task = asyncio.create_task(
                    process_downloads_async(match_ids, formatted_month)
                )
                
                # Wait for downloads to complete and send completion message
                completion_message = await download_state.download_task
                await bot.send_message(message.author, completion_message)
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error starting download: {str(e)}")
            return True

    elif content == 'dlstats':
        try:
            stats = get_download_stats()
            await bot.send_message(message.author, f"Download Stats:\n{stats}")
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error getting stats: {str(e)}")
            return True

    elif command == 'fetch':
        if len(args) != 2:
            await bot.send_message(message.author, "Usage: fetch <username>")
            return True
            
        username = args[1]
        try:
            await fetch_user_matches(username)
            await bot.send_message(message.author, f"Fetched matches for user {username}")
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error fetching matches: {str(e)}")
            return True

    elif command == 'getdemos':
        if len(args) != 2:
            await bot.send_message(message.author, "Usage: getdemos <username>")
            return True
            
        username = args[1]
        try:
            await download_user_demos(username)
            await bot.send_message(message.author, f"Downloaded demos for user {username}")
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error downloading demos: {str(e)}")
            return True
            
    elif command == 'fixids':
        # Get month if provided
        month = args[1].capitalize() if len(args) > 1 else None
        
        if month and not validate_month(month):
            await bot.send_message(message.author, "Invalid month name. Please use full month name (e.g., February).")
            return True
            
        try:
            # Run the fix_matchid_files.py script
            from commands.fix_matchid_files import fix_all_matchid_files
            
            # Load configuration from project root
            core_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # core directory
            project_dir = os.path.dirname(core_dir)  # DiscordBot directory
            config_path = os.path.join(os.path.dirname(project_dir), 'config.json')
            
            with open(config_path, 'r') as f:
                config = json.load(f)
                
            textfiles_dir = config['project']['textfiles_directory']
            
            # Fix matchid files
            results = await fix_all_matchid_files(textfiles_dir, month)
            
            # Send results
            if month:
                await bot.send_message(message.author, f"Fixed matchid files for {month}: {results['success']} successful, {results['failed']} failed, {results['skipped']} skipped\nFixed {results['fixed_entries']} entries out of {results['total_entries']} total entries")
            else:
                await bot.send_message(message.author, f"Fixed all matchid files: {results['success']} successful, {results['failed']} failed, {results['skipped']} skipped\nFixed {results['fixed_entries']} entries out of {results['total_entries']} total entries")
            return True
        except Exception as e:
            await bot.send_message(message.author, f"Error fixing matchid files: {str(e)}")
            return True

    elif command == 'stop':
        try:
            # Import here to avoid circular imports
            from commands.parser.service import parser_task, stop_parser_event
            
            # Stop download service if running
            if download_state.download_task and not download_state.download_task.done():
                # Send initial message
                await bot.send_message(message.author, "🛑 Stopping download service... Will finish current downloads but won't start new ones.")
                
                # Set the stop event to signal the downloader to stop
                download_stop_event.set()
                
                # Log the stop request
                logger.info("Stop request received for download service, waiting for current downloads to finish")
                
                # Wait for the task to complete (without a timeout)
                await download_state.download_task
                
                # Send completion message
                await bot.send_message(message.author, "✅ Download service has been gracefully stopped.")
            
            # Also stop parser service if running (it might have been started by the download loop)
            if parser_task and not parser_task.done():
                # Send initial message
                await bot.send_message(message.author, "🛑 Stopping parser service... Will finish processing current demos but won't start new ones.")
                
                # Set the stop event to signal the parser to stop
                stop_parser_event.set()
                
                # Log the stop request
                logger.info("Stop request received, waiting for current demos to finish processing")
                
                # Wait for the task to complete (without a timeout)
                await parser_task
                
                # Update service status
                bot.is_parser_running = False
                await bot.update_status()
                
                # Send completion message
                await bot.send_message(message.author, "✅ Parser service has been gracefully stopped. All files have been updated.")
            
            if not download_state.download_task or download_state.download_task.done():
                if not parser_task or parser_task.done():
                    await bot.send_message(message.author, "No services are running.")
            
            return True
        except Exception as e:
            error_msg = f"Error stopping services: {str(e)}"
            logger.error(error_msg)
            await bot.send_message(message.author, error_msg)
            return True

    return False

async def continuous_download_loop(category: str, month: str, limit: int, bot, user):
    """
    Continuously download demos in batches, waiting for parser to finish processing
    before downloading the next batch. Automatically starts the parser in loop mode
    if it's not already running.
    
    Args:
        category: 'ace' or 'quad'
        month: Month name (e.g., "February")
        limit: Maximum number of demos to download per batch, or None for all
        bot: Discord bot instance for sending messages
        user: Discord user to send messages to
    """
    try:
        # Reset the stop event to allow new downloads to start
        from core.AsyncDemoDownloader import reset_stop_event
        reset_stop_event()
        
        batch_count = 0
        total_downloaded = 0
        
        # Only log to console, not to Discord to avoid spam
        logger.info(f"Starting continuous download loop for {category} demos in {month}")
        
        # Check if parser is running, if not, start it
        from commands.parser.service import parser_task, parser_loop, stop_parser_event
        
        if not parser_stats['is_running']:
            # Start parser in continuous mode for this month
            logger.info(f"Starting parser in continuous mode for {month}")
            
            # Clear stop event in case it was set
            stop_parser_event.clear()
            
            # Default scan interval of 60 seconds (more frequent than default)
            scan_interval = 60
            
            # Start parser task - pass None for bot and user to avoid Discord messages
            parser_task = asyncio.create_task(parser_loop(month, None, 5, None, None, scan_interval))
            
            # Update bot status
            bot.is_parser_running = True
            await bot.update_status()
            
            # Wait a moment for parser to start
            await asyncio.sleep(2)
        
        while not download_stop_event.is_set():
            batch_count += 1
            
            # Get demos for this batch
            start_message, match_ids = await start_downloading_async(category, month, limit)
            
            if not match_ids:
                logger.info(f"No more demos to download for {category} in {month}. Continuous download complete.")
                break
            
            # Calculate estimated size and cost
            estimated_size_gb = len(match_ids) * 257 / 1024
            estimated_cost = estimated_size_gb * 0.03
            
            # Log batch info but don't send to Discord
            logger.info(f"Batch #{batch_count}: Downloading {len(match_ids)} {category} demos for {month}. "
                       f"Estimated size: {estimated_size_gb:.2f} GB, cost: ${estimated_cost:.2f}")
            
            # Process downloads for this batch
            completion_message = await process_downloads_async(match_ids, month)
            logger.info(f"Batch #{batch_count} complete: {completion_message}")
            
            total_downloaded += len(match_ids)
            
            # Check if stop event is set
            if download_stop_event.is_set():
                logger.info("Download loop stopped by user.")
                break
            
            # Wait until parser has finished processing all demos
            logger.info("Waiting for parser to finish processing before downloading next batch...")
            
            # Check if parser is running
            while parser_stats['is_running']:
                # Check every 10 seconds if parser is still running
                for _ in range(10):
                    if download_stop_event.is_set():
                        break
                    await asyncio.sleep(1)
                
                if download_stop_event.is_set():
                    break
                
                # Check if there are any demos in the parse queue
                from commands.parser.config import get_config
                from commands.parser.utils import async_read_file_lines
                
                config = get_config()
                textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
                if textfiles_dir:
                    month_dir = os.path.join(textfiles_dir, month)
                    month_lower = month.lower()
                    parse_queue_file = os.path.join(month_dir, f'parse_queue_{month_lower}.txt')
                    
                    if os.path.exists(parse_queue_file):
                        parse_queue = await async_read_file_lines(parse_queue_file)
                        if not parse_queue:
                            # Parse queue is empty, we can proceed with next batch
                            break
                    else:
                        # Parse queue file doesn't exist, we can proceed
                        break
                else:
                    # Can't check parse queue, wait a bit longer
                    await asyncio.sleep(30)
            
            # If parser is not running, wait a bit to make sure it's really done
            if not parser_stats['is_running']:
                await asyncio.sleep(5)
            
            # Wait a bit before starting next batch
            logger.info(f"Parser has finished processing. Starting next batch in 5 seconds...")
            await asyncio.sleep(5)
        
        # Final summary - only send this one message to Discord at the end
        await bot.send_message(user, 
            f"Continuous download complete. Downloaded {total_downloaded} demos in {batch_count} batches."
        )
        
    except Exception as e:
        logger.error(f"Error in continuous download loop: {str(e)}")
        await bot.send_message(user, f"Error in continuous download loop: {str(e)}")
    finally:
        # Make sure we're marked as done
        download_state.download_task = None

def setup(bot):
    """Required setup function for the extension"""
    return True
