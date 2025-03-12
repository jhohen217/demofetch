"""
Command handling functions for the parser module.
"""

import logging
import asyncio
import os
from typing import Dict, List, Set, Tuple, Optional

from commands.parser.service import (
    parser_task, 
    stop_parser_event, 
    parser_loop,
    start_parsing
)
from commands.parser.queue_manager import prepare_parse_queue
from commands.parser.utils import extract_uuid_from_demo_id, async_read_file_lines
from commands.parser.config import get_config
from commands.parser.rebuilder import (
    rebuild_parsed_file, rebuild_all_parsed_files, 
    rebuild_downloaded_file, rebuild_all_downloaded_files,
    rebuild_files, rebuild_all_files
)
from commands.parser.clean_duplicates import clean_parsed_files

logger = logging.getLogger('discord_bot')

async def get_eligible_demo_count(month: str) -> int:
    """
    Calculate the number of eligible demos for a month
    (downloaded & in matchids, but not parsed or rejected)
    
    Args:
        month: Month name (e.g., "February")
        
    Returns:
        int: Number of eligible demos
    """
    config = get_config()
    
    # Get file paths for this month
    textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
    if not textfiles_dir:
        return 0
    
    month_dir = os.path.join(textfiles_dir, month)
    month_lower = month.lower()
    
    files = {
        'downloaded': os.path.join(month_dir, f'downloaded_{month_lower}.txt'),
        'parsed': os.path.join(month_dir, f'parsed_{month_lower}.txt'),
        'rejected': os.path.join(month_dir, f'rejected_{month_lower}.txt'),
        'ace_matchids': os.path.join(month_dir, f'ace_matchids_{month_lower}.txt'),
        'quad_matchids': os.path.join(month_dir, f'quad_matchids_{month_lower}.txt')
    }
    
    # Read all relevant files
    downloaded_demos = await async_read_file_lines(files['downloaded'])
    parsed_demos = await async_read_file_lines(files['parsed'])
    rejected_demos = await async_read_file_lines(files['rejected'])
    
    # Read and combine ace and quad matchids
    ace_matchids = set()
    quad_matchids = set()
    
    if os.path.exists(files['ace_matchids']):
        ace_demos = await async_read_file_lines(files['ace_matchids'])
        ace_matchids = {extract_uuid_from_demo_id(demo) for demo in ace_demos}
    
    if os.path.exists(files['quad_matchids']):
        quad_demos = await async_read_file_lines(files['quad_matchids'])
        quad_matchids = {extract_uuid_from_demo_id(demo) for demo in quad_demos}
    
    matchids = ace_matchids.union(quad_matchids)
    
    # Convert to UUIDs for comparison
    downloaded_uuids = {extract_uuid_from_demo_id(demo) for demo in downloaded_demos}
    parsed_uuids = {extract_uuid_from_demo_id(demo) for demo in parsed_demos}
    rejected_uuids = {extract_uuid_from_demo_id(demo) for demo in rejected_demos}
    
    # Calculate eligible demos (downloaded & in matchids, but not parsed or rejected)
    eligible_uuids = downloaded_uuids.intersection(matchids) - parsed_uuids - rejected_uuids
    return len(eligible_uuids)

async def handle_message(bot, message):
    """Handle message-based parser commands"""
    content = message.content.lower()
    args = content.split()
    command = args[0] if args else ""
    
    global parser_task
    
    if command == 'rebuild':
        try:
            # Check if parser service is running
            if parser_task and not parser_task.done():
                await bot.send_message(message.author, "Cannot rebuild files while parser service is running. Please stop the parser service first.")
                return True
            
            # Check if download service is running
            # Import here to avoid circular imports
            from commands.DemoDownloadCommands import download_state
            if download_state.download_task and not download_state.download_task.done():
                await bot.send_message(message.author, "Cannot rebuild files while download service is running. Please wait for downloads to complete.")
                return True
            
            # Check if month parameter is provided
            if len(args) > 1:
                # For backwards compatibility, check if the first argument is 'parsed' or 'download'
                if args[1] in ['parsed', 'download']:
                    # We'll maintain the old behavior but with our new functions
                    if args[1] == 'parsed':
                        if len(args) > 2:
                            month = args[2].capitalize()
                            await bot.send_message(message.author, "The 'rebuild parsed' command is deprecated. Using 'rebuild month' instead.")
                            success, result_message = await rebuild_files(month)
                        else:
                            await bot.send_message(message.author, "The 'rebuild parsed' command is deprecated. Using 'rebuild' (all months) instead.")
                            success, result_message = await rebuild_all_files()
                    else:  # args[1] == 'download'
                        if len(args) > 2:
                            month = args[2].capitalize()
                            await bot.send_message(message.author, "The 'rebuild download' command is deprecated. Using 'rebuild month' instead.")
                            success, result_message = await rebuild_files(month)
                        else:
                            await bot.send_message(message.author, "The 'rebuild download' command is deprecated. Using 'rebuild' (all months) instead.")
                            success, result_message = await rebuild_all_files()
                else:
                    # New behavior: first argument is the month
                    month = args[1].capitalize()
                    success, result_message = await rebuild_files(month)
            else:
                # No month specified, rebuild all months
                success, result_message = await rebuild_all_files()
            
            await bot.send_message(message.author, result_message)
            return True
        except Exception as e:
            error_msg = f"Error rebuilding parsed file(s): {str(e)}"
            logger.error(error_msg)
            await bot.send_message(message.author, error_msg)
            return True
    
    elif command == 'start' and len(args) > 1 and (args[1] == 'parser' or args[1] == 'parse'):
        try:
            # Check if parser is already running
            if parser_task and not parser_task.done():
                await bot.send_message(message.author, "Parser service is already running!")
                return True
            
            # Import here to avoid circular imports
            from commands.parser.config import get_available_months
            
            # Check if month parameter is provided
            if len(args) < 3:
                # No month specified, process all months with non-empty queues
                months = get_available_months()
                if not months:
                    await bot.send_message(message.author, "No months with downloaded demos found.")
                    return True
                
                # Check each month for demos in queue and eligible demos
                months_with_demos = []
                total_demos = 0
                total_eligible = 0
                
                for month in months:
                    # Get eligible demo count using the same logic as in start_parsing
                    eligible_count = await get_eligible_demo_count(month)
                    total_eligible += eligible_count
                    
                    # Prepare parse queue for this month (no limit for checking)
                    success, prep_stats = await prepare_parse_queue(month, stop_parser_event, None)
                    if success and prep_stats['queue_size'] > 0:
                        months_with_demos.append((month, prep_stats['queue_size'], eligible_count))
                        total_demos += prep_stats['queue_size']
                
                if not months_with_demos:
                    if total_eligible > 0:
                        await bot.send_message(message.author, f"No demos in queue for any month. {total_eligible} eligible demos available - run 'rebuild' to queue them for parsing.")
                    else:
                        await bot.send_message(message.author, "No demos in queue for any month.")
                    return True
                
                # Get parallel limit from args if provided (default to 5)
                parallel_limit = 5
                scan_interval = 300  # Default to 5 minutes
                
                if len(args) >= 3:
                    try:
                        parallel_limit = int(args[2])
                        if parallel_limit <= 0:
                            await bot.send_message(message.author, "Parallel limit must be positive.")
                            return True
                    except ValueError:
                        # Not a number, might be a month name, so use default
                        pass
                
                # Check if scan interval is provided
                if len(args) >= 4:
                    try:
                        scan_interval = int(args[3])
                        if scan_interval < 30:  # Minimum 30 seconds
                            await bot.send_message(message.author, "Scan interval must be at least 30 seconds.")
                            return True
                    except ValueError:
                        await bot.send_message(message.author, "Invalid scan interval. Please use a number of seconds (minimum 30).")
                        return True
                
                # Inform user about what will be processed
                months_info = ", ".join([f"{month} ({count} demos in queue, {eligible} eligible)" for month, count, eligible in months_with_demos])
                start_message = f"Starting continuous parsing of demos across {len(months_with_demos)} months: {months_info}"
                start_message += f"\nScanning for new demos every {scan_interval} seconds with {parallel_limit} parallel processes."
                await bot.send_message(message.author, start_message)
                
                # Start parser service for all months
                stop_parser_event.clear()
                # Pass the bot and user for completion notification
                parser_task = asyncio.create_task(parser_loop(None, None, parallel_limit, bot, message.author, scan_interval))
                
                # Update service status
                bot.is_parser_running = True
                await bot.update_status()
                
                return True
            
            month = args[2].capitalize()
            
            # Check if number parameter is provided
            limit = None
            if len(args) >= 4:
                try:
                    if args[3].lower() == 'all':
                        limit = None
                    else:
                        limit = int(args[3])
                        if limit <= 0:
                            await bot.send_message(message.author, "Number must be positive or 'all'.")
                            return True
                except ValueError:
                    await bot.send_message(message.author, "Invalid number. Please use a positive number or 'all'.")
                    return True
            
            # Start parsing for the specified month and limit
            start_message = await start_parsing(month, limit)
            await bot.send_message(message.author, start_message)
            
            if "Error" in start_message or "No demos" in start_message:
                return True
            
            # Get parallel limit from args if provided (default to 5)
            parallel_limit = 5
            if len(args) >= 5:
                try:
                    parallel_limit = int(args[4])
                    if parallel_limit <= 0:
                        await bot.send_message(message.author, "Parallel limit must be positive.")
                        return True
                except ValueError:
                    await bot.send_message(message.author, "Invalid parallel limit. Please use a positive number.")
                    return True
            
            # Get scan interval if provided (default to 5 minutes)
            scan_interval = 300
            if len(args) >= 6:
                try:
                    scan_interval = int(args[5])
                    if scan_interval < 30:  # Minimum 30 seconds
                        await bot.send_message(message.author, "Scan interval must be at least 30 seconds.")
                        return True
                except ValueError:
                    await bot.send_message(message.author, "Invalid scan interval. Please use a number of seconds (minimum 30).")
                    return True
            
            # Update start message to include continuous scanning info
            start_message += f"\nContinuously scanning for new demos every {scan_interval} seconds with {parallel_limit} parallel processes."
            await bot.send_message(message.author, start_message)
            
            # Start parser service with specific month, limit, parallel limit, and scan interval
            stop_parser_event.clear()
            # Pass the bot and user for completion notification
            parser_task = asyncio.create_task(parser_loop(month, limit, parallel_limit, bot, message.author, scan_interval))
            
            # Update service status
            bot.is_parser_running = True
            await bot.update_status()
            
            return True
            
        except PermissionError as e:
            if "System Volume Information" in str(e):
                error_msg = "Error starting parser service: Access denied to 'System Volume Information' directory. This is a protected system directory. The parser has been updated to handle this error gracefully. Please try again."
            else:
                error_msg = f"Permission denied when starting parser service: {str(e)}"
            logger.error(error_msg)
            await bot.send_message(message.author, error_msg)
            return True
        except Exception as e:
            error_msg = f"Error starting parser service: {str(e)}"
            logger.error(error_msg)
            await bot.send_message(message.author, error_msg)
            return True
    
    elif command == 'clean' and len(args) > 1 and args[1] == 'duplicates':
        try:
            # Check if parser service is running
            if parser_task and not parser_task.done():
                await bot.send_message(message.author, "Cannot clean duplicates while parser service is running. Please stop the parser service first.")
                return True
            
            # Check if download service is running
            # Import here to avoid circular imports
            from commands.DemoDownloadCommands import download_state
            if download_state.download_task and not download_state.download_task.done():
                await bot.send_message(message.author, "Cannot clean duplicates while download service is running. Please wait for downloads to complete.")
                return True
            
            # Send initial message
            await bot.send_message(message.author, "🧹 Cleaning up duplicate entries in parsed files...")
            
            # Check if month parameter is provided
            months = None
            if len(args) > 2:
                months = [args[2].capitalize()]
            
            # Run the cleanup
            await clean_parsed_files(months)
            
            # Send completion message
            if months:
                await bot.send_message(message.author, f"✅ Duplicates cleaned up for {months[0]}!")
            else:
                await bot.send_message(message.author, "✅ Duplicates cleaned up for all months!")
            
            return True
        except Exception as e:
            error_msg = f"Error cleaning duplicates: {str(e)}"
            logger.error(error_msg)
            await bot.send_message(message.author, error_msg)
            return True
    
    elif command == 'stop' and (len(args) == 1 or (len(args) > 1 and args[1] == 'parser')):
        try:
            # Stop parser service if running
            if parser_task and not parser_task.done():
                # Send initial message
                await bot.send_message(message.author, "🛑 Stopping parser service... Will finish processing current demos but won't start new ones.")
                
                # Set the stop event to signal the parser to stop
                stop_parser_event.set()
                
                # Log the stop request
                logger.info("Stop request received, waiting for current demos to finish processing")
                
                # Immediately cancel the task without waiting
                logger.info("Cancelling parser task immediately")
                parser_task.cancel()
                try:
                    await parser_task
                except asyncio.CancelledError:
                    logger.info("Parser task cancelled successfully")
                except Exception as e:
                    logger.error(f"Error cancelling parser task: {str(e)}")
                
                # Reset the parser_task variable to None
                parser_task = None
                
                # Update service status
                bot.is_parser_running = False
                await bot.update_status()
                
                # Send completion message
                await bot.send_message(message.author, "✅ Parser service has been gracefully stopped. All files have been updated.")
            else:
                await bot.send_message(message.author, "Parser service is not running.")
            
            # If this is just 'stop' without specifying 'parser', also stop the download service
            if len(args) == 1:
                # Import here to avoid circular imports
                from commands.DemoDownloadCommands import download_state
                from core.AsyncDemoDownloader import stop_event as download_stop_event
                
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
            
            return True if len(args) > 1 and args[1] == 'parser' else False
            
        except Exception as e:
            error_msg = f"Error stopping parser service: {str(e)}"
            logger.error(error_msg)
            await bot.send_message(message.author, error_msg)
            return True if len(args) > 1 and args[1] == 'parser' else False
    
    return False

def setup(bot):
    """Required setup function for the extension"""
    logger.info("Parser commands module setup")
    bot.is_parser_running = False
    return True
