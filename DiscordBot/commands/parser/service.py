"""
Service functions for the CS2 demo parser.
"""

import logging
import asyncio
import os
import time
from datetime import datetime
from typing import Dict, Optional

from commands.parser.utils import format_time_duration, alphabetize_file, extract_uuid_from_demo_id
from commands.parser.config import get_config, get_available_months
from commands.parser.queue_manager import prepare_parse_queue_async, prepare_parse_queue
from commands.parser.batch_processor import process_month_queue_async

logger = logging.getLogger('discord_bot')

# Store background tasks
parser_task = None
stop_parser_event = asyncio.Event()

# Parser stats
parser_stats = {
    'total_processed': 0,
    'successful': 0,
    'failed': 0,
    'last_demo_id': None,
    'is_running': False,
    'last_check_time': None,
    'current_month': None,
    'kill_collections': 0,
    'tickbytick_files': 0,
    'processing_time': 0
}

def get_parser_stats():
    """Get current parser statistics"""
    return parser_stats

async def parser_loop(specific_month: str = None, limit: int = None, parallel_limit: int = 5, 
                     discord_bot = None, discord_user = None, scan_interval: int = 300):
    """
    Main parser loop that runs continuously, scanning for new demos to process
    
    Args:
        specific_month: Optional month to process (e.g., "February")
                        If None, process all months with demos in queue
        limit: Maximum number of demos to process per scan, or None for all
        parallel_limit: Maximum number of demos to process in parallel
        discord_bot: Optional Discord bot instance for sending completion notifications
        discord_user: Optional Discord user to notify when parsing is complete
        scan_interval: Time in seconds to wait between scans for new demos (default: 5 minutes)
    """
    # Set up signal handlers for graceful shutdown
    import signal
    
    def signal_handler():
        logger.info("Received termination signal, setting stop event")
        stop_parser_event.set()
    
    # Register signal handlers for graceful shutdown
    try:
        for sig in (signal.SIGINT, signal.SIGTERM):
            asyncio.get_event_loop().add_signal_handler(sig, signal_handler)
        logger.info("Registered signal handlers for graceful shutdown")
    except NotImplementedError:
        # Windows doesn't support add_signal_handler
        logger.info("Signal handlers not supported on this platform")
        
    # Check if stop event is already set (in case this was called after a stop command)
    if stop_parser_event.is_set():
        logger.info("Stop event already set, not starting parser loop")
        parser_stats['is_running'] = False
        return
    try:
        logger.info(f"Starting continuous parser loop{' for ' + specific_month if specific_month else ''}")
        print(f"Starting continuous parser loop{' for ' + specific_month if specific_month else ''}")
        parser_stats['is_running'] = True
        
        # Reset stats
        parser_stats['kill_collections'] = 0
        parser_stats['tickbytick_files'] = 0
        parser_stats['processing_time'] = 0
        
        # Get configuration
        config = get_config()
        
        # Main continuous loop - runs until stop event is set
        while not stop_parser_event.is_set():
            scan_start_time = time.time()
            logger.info(f"Starting new scan for demos to parse")
            
            # If specific month is provided, only process that month
            if specific_month:
                parser_stats['current_month'] = specific_month
                
                # Check if stop event is set before preparing queue
                if stop_parser_event.is_set():
                    break
                
                # Prepare parse queue with limit
                success, prep_stats = await prepare_parse_queue(specific_month, stop_parser_event, limit)
                
                if not success:
                    logger.error(f"Failed to prepare parse queue for {specific_month}")
                    print(f"Failed to prepare parse queue for {specific_month}")
                else:
                    if prep_stats['queue_size'] > 0:
                        logger.info(f"Found {prep_stats['queue_size']} demos to parse for {specific_month}")
                        print(f"Found {prep_stats['queue_size']} demos to parse for {specific_month}")
                        
                        # Check if stop event is set before processing queue
                        if stop_parser_event.is_set():
                            break
                        
                        # Process the queue
                        stats, completion_message = await process_month_queue_async(
                            specific_month, config, stop_parser_event, parser_stats, limit, parallel_limit
                        )
                        
                        logger.info(f"Processed {stats['processed']} demos for {specific_month}: "
                                    f"{stats['successful']} successful, "
                                    f"{stats['failed']} failed, "
                                    f"{stats['skipped']} skipped")
                        
                        # Alphabetize the text files for this month
                        textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
                        if textfiles_dir:
                            month_dir = os.path.join(textfiles_dir, specific_month)
                            month_lower = specific_month.lower()
                            
                            # Alphabetize the parsed, parse_queue, and downloaded files
                            await alphabetize_file(os.path.join(month_dir, f'parsed_{month_lower}.txt'))
                            await alphabetize_file(os.path.join(month_dir, f'parse_queue_{month_lower}.txt'), preserve_chronological=True)
                            await alphabetize_file(os.path.join(month_dir, f'downloaded_{month_lower}.txt'))
                            
                            # Also alphabetize the ace_matchids and quad_matchids files if they exist
                            ace_matchids_file = os.path.join(month_dir, f'ace_matchids_{month_lower}.txt')
                            quad_matchids_file = os.path.join(month_dir, f'quad_matchids_{month_lower}.txt')
                            
                            if os.path.exists(ace_matchids_file):
                                await alphabetize_file(ace_matchids_file, preserve_chronological=True)
                            
                            if os.path.exists(quad_matchids_file):
                                await alphabetize_file(quad_matchids_file, preserve_chronological=True)
                            
                            logger.info(f"Alphabetized text files for {specific_month}")
                        
                        # Send completion notification if Discord bot and user are provided
                        # Only send if not in continuous mode (scan_interval == 0 means one-time run)
                        if discord_bot and discord_user and stats['processed'] > 0 and scan_interval == 0:
                            notification_message = (
                                f"🎉 **Parsing Complete for {specific_month}!** 🎉\n\n"
                                f"Successfully parsed {stats['successful']} demos, "
                                f"failed {stats['failed']}, "
                                f"skipped {stats['skipped']}.\n"
                                f"Generated {parser_stats['kill_collections']} kill collections and "
                                f"{parser_stats['tickbytick_files']} tickbytick files "
                                f"in {format_time_duration(parser_stats['processing_time'])}."
                            )
                            
                            try:
                                await discord_bot.send_message(discord_user, notification_message)
                                logger.info(f"Sent completion notification to {discord_user}")
                            except Exception as e:
                                logger.error(f"Failed to send completion notification: {str(e)}")
                    else:
                        logger.info(f"No new demos to parse for {specific_month}")
                        print(f"No new demos to parse for {specific_month}")
            
            # No specific month provided, process all months with demos in queue
            else:
                try:
                    # Get available months
                    months = get_available_months()
                    
                    if not months:
                        logger.warning("No months with downloaded demos found")
                        print("No months with downloaded demos found")
                    else:
                        total_processed = 0
                        
                        # Process each month
                        for month in months:
                            # Check if stop event is set before processing each month
                            if stop_parser_event.is_set():
                                break
                            
                            parser_stats['current_month'] = month
                            
                            # Prepare parse queue (no limit for all months mode)
                            success, prep_stats = await prepare_parse_queue(month, stop_parser_event)
                            
                            if not success:
                                logger.error(f"Failed to prepare parse queue for {month}")
                                continue
                            
                            if prep_stats['queue_size'] == 0:
                                logger.info(f"No demos to parse for {month}")
                                continue
                            
                            logger.info(f"Found {prep_stats['queue_size']} demos to parse for {month}")
                            print(f"Found {prep_stats['queue_size']} demos to parse for {month}")
                            
                            # Check if stop event is set before processing queue
                            if stop_parser_event.is_set():
                                break
                            
                            # Process the queue
                            stats, completion_message = await process_month_queue_async(
                                month, config, stop_parser_event, parser_stats, None, parallel_limit
                            )
                            
                            total_processed += stats['processed']
                            
                            logger.info(f"Processed {stats['processed']} demos for {month}: "
                                        f"{stats['successful']} successful, "
                                        f"{stats['failed']} failed, "
                                        f"{stats['skipped']} skipped")
                            
                            # Alphabetize the text files for this month
                            textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
                            if textfiles_dir:
                                month_dir = os.path.join(textfiles_dir, month)
                                month_lower = month.lower()
                                
                                # Alphabetize the parsed, parse_queue, and downloaded files
                                await alphabetize_file(os.path.join(month_dir, f'parsed_{month_lower}.txt'))
                                await alphabetize_file(os.path.join(month_dir, f'parse_queue_{month_lower}.txt'), preserve_chronological=True)
                                await alphabetize_file(os.path.join(month_dir, f'downloaded_{month_lower}.txt'))
                                
                                logger.info(f"Alphabetized text files for {month}")
                            
                            print(completion_message)
                        
                        # Send completion notification if Discord bot and user are provided and demos were processed
                        # Only send if not in continuous mode (scan_interval == 0 means one-time run)
                        if discord_bot and discord_user and total_processed > 0 and scan_interval == 0:
                            total_stats = {
                                'total_processed': parser_stats['total_processed'],
                                'successful': parser_stats['successful'],
                                'failed': parser_stats['failed'],
                                'kill_collections': parser_stats['kill_collections'],
                                'tickbytick_files': parser_stats['tickbytick_files'],
                                'processing_time': parser_stats['processing_time']
                            }
                            
                            notification_message = (
                                f"🎉 **Parsing Complete!** 🎉\n\n"
                                f"Successfully parsed {total_stats['successful']} demos, "
                                f"failed {total_stats['failed']}.\n"
                                f"Generated {total_stats['kill_collections']} kill collections and "
                                f"{total_stats['tickbytick_files']} tickbytick files "
                                f"in {format_time_duration(total_stats['processing_time'])}."
                            )
                            
                            try:
                                await discord_bot.send_message(discord_user, notification_message)
                                logger.info(f"Sent completion notification to {discord_user}")
                            except Exception as e:
                                logger.error(f"Failed to send completion notification: {str(e)}")
                        
                        if total_processed == 0:
                            logger.info("No new demos to parse for any month")
                            print("No new demos to parse for any month")
                    
                except Exception as e:
                    logger.error(f"Error processing all months: {str(e)}")
                    print(f"Error processing all months: {str(e)}")
            
            # Check if stop event is set before sleeping
            if stop_parser_event.is_set():
                logger.info("Stop event detected, breaking out of parser loop")
                break
            
            # Calculate time to sleep (scan_interval minus time spent processing)
            elapsed_time = time.time() - scan_start_time
            sleep_time = max(1, scan_interval - elapsed_time)
            
            logger.info(f"Scan complete. Sleeping for {int(sleep_time)} seconds before next scan...")
            print(f"Scan complete. Sleeping for {int(sleep_time)} seconds before next scan...")
            
            # Sleep with periodic checks for stop event
            sleep_interval = 1  # Check stop event every second
            for _ in range(int(sleep_time / sleep_interval)):
                if stop_parser_event.is_set():
                    logger.info("Stop event detected during sleep, breaking out of parser loop")
                    break
                await asyncio.sleep(sleep_interval)
            
            # Check stop event again before continuing to next iteration
            if stop_parser_event.is_set():
                logger.info("Stop event detected after sleep, breaking out of parser loop")
                break
                
            # Sleep any remaining time
            remaining_sleep = sleep_time % sleep_interval
            if remaining_sleep > 0 and not stop_parser_event.is_set():
                await asyncio.sleep(remaining_sleep)
                
            # Final check before next iteration
            if stop_parser_event.is_set():
                logger.info("Stop event detected after final sleep, breaking out of parser loop")
                break
    
    except asyncio.CancelledError:
        logger.info("Parser task cancelled")
    
    finally:
        parser_stats['is_running'] = False
        parser_stats['current_month'] = None
        logger.info("Parser loop stopped")
        print("Parser loop stopped")

async def start_parsing(month: str, limit: int = None) -> str:
    """
    Start parsing demos from the specified month
    
    Args:
        month: Month name (e.g., "February")
        limit: Maximum number of demos to parse, or None for all
        
    Returns:
        str: Initial message about the parsing operation
    """
    # Validate month
    month = month.capitalize()
    valid_months = get_available_months()
    if month not in valid_months:
        return f"Error: Invalid month '{month}'. Available months: {', '.join(valid_months)}"
    
    # Get configuration
    config = get_config()
    
    # Get file paths for this month
    textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
    if not textfiles_dir:
        return "Error: Textfiles directory not found in config"
    
    month_dir = os.path.join(textfiles_dir, month)
    month_lower = month.lower()
    
    files = {
        'downloaded': os.path.join(month_dir, f'downloaded_{month_lower}.txt'),
        'parsed': os.path.join(month_dir, f'parsed_{month_lower}.txt'),
        'rejected': os.path.join(month_dir, f'rejected_{month_lower}.txt'),
        'ace_matchids': os.path.join(month_dir, f'ace_matchids_{month_lower}.txt'),
        'quad_matchids': os.path.join(month_dir, f'quad_matchids_{month_lower}.txt')
    }
    
    # Calculate eligible demos (downloaded but not parsed or rejected)
    from commands.parser.utils import async_read_file_lines
    
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
    eligible_count = len(eligible_uuids)
    
    # Prepare parse queue with limit
    success, prep_stats = await prepare_parse_queue(month, stop_parser_event, limit)
    
    if not success:
        if 'error' in prep_stats:
            return f"Error: {prep_stats['error']}"
        return f"No demos in queue for {month}. {eligible_count} eligible demos available - run 'rebuild {month}' to queue them for parsing."
    
    if prep_stats['queue_size'] == 0:
        return f"No demos in queue for {month}. {eligible_count} eligible demos available - run 'rebuild {month}' to queue them for parsing."
    
    # Apply limit if specified
    queue_size = prep_stats['queue_size']
    if limit is not None and limit > 0 and limit < queue_size:
        queue_size = limit
        remaining = eligible_count - queue_size
        return f"Starting parsing of {queue_size} demos for {month}... ({remaining} more demos eligible for parsing)"
    
    remaining = eligible_count - queue_size
    if remaining > 0:
        return f"Starting parsing of {queue_size} demos for {month}... ({remaining} more demos eligible for parsing)"
    else:
        return f"Starting parsing of {queue_size} demos for {month}..."
