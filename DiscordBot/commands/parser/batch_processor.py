"""
Functions for processing batches of CS2 demo files.
"""

import os
import time
import logging
import asyncio
import aiofiles
from typing import Dict, List, Set, Tuple, Optional

from commands.parser.utils import (
    format_match_id,
    format_time_duration,
    async_read_file_lines,
    async_write_file_lines,
    async_append_file_line,
    retry_operation,
    extract_uuid_from_demo_id
)
from commands.parser.config import get_config, get_demo_path_async
from commands.parser.demo_processor import process_demo

logger = logging.getLogger('discord_bot')

async def process_month_queue_async(month: str, config: Dict, stop_event: asyncio.Event, 
                                   parser_stats: Dict, limit: int = None, parallel_limit: int = 5) -> Tuple[Dict, str]:
    """
    Process the parsing queue for a specific month with parallel processing
    
    Args:
        month: Month name (e.g., "February")
        config: Configuration dictionary
        stop_event: Event to signal stopping
        parser_stats: Dictionary to track parser statistics
        limit: Maximum number of demos to process, or None for all
        parallel_limit: Maximum number of demos to process in parallel
        
    Returns:
        Tuple[Dict, str]: Processing statistics and completion message
    """
    stats = {
        'processed': 0,
        'successful': 0,
        'failed': 0,
        'skipped': 0
    }
    
    # Get file paths
    textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
    if not textfiles_dir:
        logger.error("Textfiles directory not found in config")
        return stats, "Error: Textfiles directory not found in config"
    
    month_dir = os.path.join(textfiles_dir, month)
    month_lower = month.lower()
    
    files = {
        'dir': month_dir,
        'downloaded': os.path.join(month_dir, f'downloaded_{month_lower}.txt'),
        'parsed': os.path.join(month_dir, f'parsed_{month_lower}.txt'),
        'parse_queue': os.path.join(month_dir, f'parse_queue_{month_lower}.txt'),
        'rejected': os.path.join(month_dir, f'rejected_{month_lower}.txt'),
        'ace_matchids': os.path.join(month_dir, f'ace_matchids_{month_lower}.txt'),
        'quad_matchids': os.path.join(month_dir, f'quad_matchids_{month_lower}.txt')
    }
    
    # Create rejected file if it doesn't exist
    if not os.path.exists(files['rejected']):
        async with aiofiles.open(files['rejected'], 'w', encoding='utf-8') as f:
            pass
    
    # Create parsed file if it doesn't exist
    if not os.path.exists(files['parsed']):
        async with aiofiles.open(files['parsed'], 'w', encoding='utf-8') as f:
            pass
    
    # Read queue with retry
    queue = list(await retry_operation(
        lambda: async_read_file_lines(files['parse_queue'])
    ))
    
    if not queue:
        logger.info(f"No demos in queue for {month}")
        return stats, f"No demos in queue for {month}"
    
    # Apply limit if specified
    # Log summary of demos to process
    if limit is not None and limit > 0 and limit < len(queue):
        queue = queue[:limit]
        logger.info(f"Processing {len(queue)} demos for {month} (limited to {limit})")
        print(f"Processing queue for {month}: {len(queue)} demos (limited to {limit})")
    else:
        logger.info(f"Processing {len(queue)} demos for {month}")
        print(f"Processing queue for {month}: {len(queue)} demos")
    
    # Track start time for this batch
    start_time = time.time()
    
    # Reset batch stats
    batch_kill_collections = 0
    batch_tickbytick_files = 0
    
    # Create a semaphore to limit concurrent processing
    semaphore = asyncio.Semaphore(parallel_limit)
    
    # Create a lock for synchronizing file updates
    queue_update_lock = asyncio.Lock()
    
    # Variables for periodic queue updates
    update_interval = 10  # Update queue every 10 successful demos
    successful_count = 0
    last_queue_update_time = time.time()
    time_update_interval = 60  # Update queue every 60 seconds
    
    # Shared collections for tracking demo status
    successful_demos = []
    failed_demos = []
    skipped_demos = []
    
    async def process_demo_with_semaphore(demo_id: str):
        """Process a demo with semaphore control"""
        nonlocal batch_kill_collections, batch_tickbytick_files, successful_count, last_queue_update_time
        
        # Check if stop event is set
        if stop_event.is_set():
            logger.info(f"Stop event detected, skipping demo: {demo_id}")
            return demo_id, False
        
        # Extract UUID from demo_id (function imported at the top level)
        demo_uuid = extract_uuid_from_demo_id(demo_id)
        
        # Read parsed demos
        parsed_demos = await retry_operation(
            lambda: async_read_file_lines(files['parsed'])
        )
        
        # Check if any parsed demo has the same UUID
        already_parsed = False
        for parsed_demo in parsed_demos:
            parsed_uuid = extract_uuid_from_demo_id(parsed_demo)
            if parsed_uuid == demo_uuid:
                already_parsed = True
                break
        
        if already_parsed:
            logger.info(f"Demo already parsed (UUID match): {demo_id}")
            print(f"[✓] Skipped demo: {format_match_id(demo_id)} - Already parsed (UUID match)")
            
            # Add to skipped demos
            async with queue_update_lock:
                skipped_demos.append(demo_id)
                
            return demo_id, None  # None indicates skipped
        
        # Get demo path
        demo_path = await get_demo_path_async(demo_id, month)
        if not demo_path:
            logger.warning(f"Demo file not found for ID: {demo_id}")
            print(f"[✗] Skipped demo: {format_match_id(demo_id)} - File not found")
            
            # Add to skipped demos
            async with queue_update_lock:
                skipped_demos.append(demo_id)
                
            return demo_id, None  # None indicates skipped
        
        # Track kill collections and tickbytick files before processing
        kill_collections_before = parser_stats['kill_collections']
        tickbytick_files_before = parser_stats['tickbytick_files']
        
        # Process the demo with semaphore
        async with semaphore:
            success = await process_demo(demo_id, demo_path, month, parser_stats, stop_event)
        
        # Calculate how many new files were generated
        batch_kill_collections += (parser_stats['kill_collections'] - kill_collections_before)
        batch_tickbytick_files += (parser_stats['tickbytick_files'] - tickbytick_files_before)
        
        # Update parser stats
        parser_stats['total_processed'] += 1
        parser_stats['last_demo_id'] = demo_id
        
        if success:
            parser_stats['successful'] += 1
            
            # Add to successful demos - but DON'T update parsed file yet
            # We'll update it at the end of batch processing to avoid race conditions
            async with queue_update_lock:
                successful_demos.append(demo_id)
                
                # Just increment successful count
                successful_count += 1
                
                # Check if we should update the queue file - more aggressive updates
                current_time = time.time()
                if (successful_count >= 5 or 
                    current_time - last_queue_update_time >= 30):  # Update every 30 seconds instead of 60
                    
                    # Read current queue
                    current_queue = list(await retry_operation(
                        lambda: async_read_file_lines(files['parse_queue'])
                    ))
                    
                    # Remove successfully processed and skipped demos
                    updated_queue = [d for d in current_queue if d not in successful_demos and d not in skipped_demos]
                    
                    # Write updated queue
                    await retry_operation(
                        lambda: async_write_file_lines(files['parse_queue'], updated_queue, use_temp_file=True)
                    )
                    
                    # Reset counters
                    successful_count = 0
                    last_queue_update_time = current_time
                    
                    # Log update
                    removed_count = len(current_queue) - len(updated_queue)
                    logger.info(f"Updated queue file: removed {removed_count} demos, {len(updated_queue)} remaining")
                    print(f"[✓] Updated queue: removed {removed_count} demos, {len(updated_queue)} remaining")
        else:
            parser_stats['failed'] += 1
            
            # Add to failed demos and update rejected file
            async with queue_update_lock:
                failed_demos.append(demo_id)
                
                # Add to rejected file with retry
                await retry_operation(
                    lambda: async_append_file_line(files['rejected'], demo_id)
                )
                
                # Remove from queue immediately to prevent further attempts
                current_queue = list(await retry_operation(
                    lambda: async_read_file_lines(files['parse_queue'])
                ))
                
                if demo_id in current_queue:
                    updated_queue = [d for d in current_queue if d != demo_id]
                    await retry_operation(
                        lambda: async_write_file_lines(files['parse_queue'], updated_queue, use_temp_file=True)
                    )
                    logger.info(f"Removed failed demo from queue: {demo_id}")
                
                # Remove from ace_matchids and quad_matchids files by UUID
                for matchid_file in [files['ace_matchids'], files['quad_matchids']]:
                    if os.path.exists(matchid_file):
                        try:
                            # Read the file
                            matchids = list(await retry_operation(
                                lambda: async_read_file_lines(matchid_file)
                            ))
                            
                            # Filter out entries with the same UUID
                            updated_matchids = []
                            for matchid in matchids:
                                matchid_uuid = extract_uuid_from_demo_id(matchid)
                                if matchid_uuid != demo_uuid:
                                    updated_matchids.append(matchid)
                                else:
                                    logger.info(f"Removed failed demo from {os.path.basename(matchid_file)}: {matchid} (UUID: {demo_uuid})")
                            
                            # Write back if any were removed
                            if len(updated_matchids) != len(matchids):
                                # Sort the updated matchids before writing back
                                updated_matchids.sort()
                                
                                await retry_operation(
                                    lambda: async_write_file_lines(matchid_file, updated_matchids, use_temp_file=True)
                                )
                                
                                # Also alphabetize the file to ensure it's properly sorted
                                from commands.parser.utils import alphabetize_file
                                await alphabetize_file(matchid_file, remove_duplicates=True)
                                logger.info(f"Alphabetized {os.path.basename(matchid_file)} after removing failed demo")
                        except Exception as e:
                            logger.error(f"Error removing failed demo from {os.path.basename(matchid_file)}: {str(e)}")
                
                # Delete the .dem and .dem.gz files
                try:
                    # Get the demo path
                    demos_dir = config.get('project', {}).get('public_demos_directory', '')
                    if demos_dir:
                        month_dir = os.path.join(demos_dir, month)
                        if os.path.exists(month_dir):
                            try:
                                # Try to find the demo file by UUID
                                for filename in os.listdir(month_dir):
                                    if demo_uuid in filename:
                                        try:
                                            # Delete .dem file
                                            dem_path = os.path.join(month_dir, filename)
                                            if os.path.exists(dem_path):
                                                os.remove(dem_path)
                                                logger.info(f"Deleted failed demo file: {dem_path}")
                                            
                                            # Delete .dem.gz file if it exists
                                            gz_path = dem_path + '.gz'
                                            if os.path.exists(gz_path):
                                                os.remove(gz_path)
                                                logger.info(f"Deleted failed demo archive file: {gz_path}")
                                        except PermissionError as e:
                                            logger.warning(f"Permission denied when deleting demo file {dem_path}: {str(e)}")
                                        except Exception as e:
                                            logger.error(f"Error deleting demo file {dem_path}: {str(e)}")
                            except PermissionError as e:
                                logger.warning(f"Permission denied when listing directory {month_dir}: {str(e)}")
                            except Exception as e:
                                logger.error(f"Error listing directory {month_dir}: {str(e)}")
                except PermissionError as e:
                    logger.warning(f"Permission denied when accessing demos directory: {str(e)}")
                except Exception as e:
                    logger.error(f"Error deleting failed demo files: {str(e)}")
        
        return demo_id, success
    
    # Process demos in parallel
    tasks = []
    for demo_id in queue:
        if stop_event.is_set():
            logger.info("Stop event detected, not queuing any more demos")
            print("[!] Stop requested, finishing current demos but not starting new ones")
            break
        tasks.append(asyncio.create_task(process_demo_with_semaphore(demo_id)))
    
    # Wait for all tasks to complete
    results = []
    for task in asyncio.as_completed(tasks):
        try:
            result = await task
            results.append(result)
        except Exception as e:
            logger.error(f"Error in demo processing task: {str(e)}")
    
    # Process results and update stats
    for demo_id, success in results:
        stats['processed'] += 1
        
        if success is None:  # Skipped
            stats['skipped'] += 1
        elif success:  # Successful
            stats['successful'] += 1
        else:  # Failed
            stats['failed'] += 1
    
    # Final update of queue file - force removal of all processed demos
    try:
        current_queue = list(await retry_operation(
            lambda: async_read_file_lines(files['parse_queue'])
        ))
        
        # Explicitly remove all successful and skipped demos from queue
        updated_queue = [d for d in current_queue if d not in successful_demos and d not in skipped_demos]
        
        # Write updated queue with retry
        await retry_operation(
            lambda: async_write_file_lines(files['parse_queue'], updated_queue, use_temp_file=True)
        )
        
        # Log final queue update
        removed_count = len(current_queue) - len(updated_queue)
        logger.info(f"Final queue update: removed {removed_count} demos, {len(updated_queue)} remain in queue")
        print(f"[✓] Final queue update: removed {removed_count} demos, {len(updated_queue)} remain in queue")
    except Exception as e:
        logger.error(f"Error in final queue update: {str(e)}")
        print(f"[!] Error in final queue update: {str(e)}")
    
    # Final alphabetize of the rejected file
    from commands.parser.utils import alphabetize_file
    await alphabetize_file(files['rejected'], remove_duplicates=True)
    logger.info(f"Final alphabetization and duplicate removal for {month} rejected file")
    
    # Update the parsed file with all successfully processed demos
    # This is done at the end to avoid race conditions
    if successful_demos:
        logger.info(f"Adding {len(successful_demos)} successfully processed demos to parsed file")
        
        # Read existing parsed file
        parsed_demos = await retry_operation(
            lambda: async_read_file_lines(files['parsed'])
        )
        
        # Extract UUIDs from successful demos
        for demo_id in successful_demos:
            # Remove prefix from demo_id before adding to parsed file
            parsed_demo_id = extract_uuid_from_demo_id(demo_id)
            parsed_demos.add(parsed_demo_id)
        
        # Write all parsed demos back to file
        await retry_operation(
            lambda: async_write_file_lines(files['parsed'], list(parsed_demos), use_temp_file=True)
        )
        
        logger.info(f"Updated parsed file with {len(successful_demos)} new demos")
    
    # Final alphabetize of the parsed file to remove any duplicates
    from commands.parser.utils import alphabetize_file
    await alphabetize_file(files['parsed'], remove_duplicates=True)
    logger.info(f"Final alphabetization and duplicate removal for {month} parsed file")
    
    # Final alphabetize of the ace_matchids and quad_matchids files
    for matchid_file in [files['ace_matchids'], files['quad_matchids']]:
        if os.path.exists(matchid_file):
            await alphabetize_file(matchid_file, remove_duplicates=True, preserve_chronological=True)
            logger.info(f"Final alphabetization and duplicate removal for {os.path.basename(matchid_file)}")
    
    # Calculate elapsed time
    elapsed_time = time.time() - start_time
    parser_stats['processing_time'] += elapsed_time
    
    # Log summary if any demos were processed
    if stats['processed'] > 0:
        print(f"Parsed {batch_kill_collections} kill collections and {batch_tickbytick_files} tickbytick csvs in {format_time_duration(elapsed_time)}")
    
    # Return completion message
    completion_message = (
        f"Parsing complete for {month}! "
        f"Successfully parsed {stats['successful']} demos, "
        f"failed {stats['failed']}, "
        f"skipped {stats['skipped']}. "
        f"Generated {batch_kill_collections} kill collections and {batch_tickbytick_files} tickbytick files "
        f"in {format_time_duration(elapsed_time)}."
    )
    print(completion_message)
    
    return stats, completion_message

async def process_month_queue(month: str, limit: int = None, stop_event: asyncio.Event = None, 
                             parser_stats: Dict = None) -> Tuple[Dict, str]:
    """
    Process the parsing queue for a specific month (wrapper for async version)
    
    Args:
        month: Month name (e.g., "February")
        limit: Maximum number of demos to process, or None for all
        stop_event: Optional event to signal stopping
        parser_stats: Optional dictionary to track parser statistics
        
    Returns:
        Tuple[Dict, str]: Processing statistics and completion message
    """
    config = get_config()
    
    # If stop_event or parser_stats are not provided, import from service
    if stop_event is None or parser_stats is None:
        from commands.parser.service import stop_parser_event as service_stop_event, parser_stats as service_parser_stats
        stop_event = stop_event or service_stop_event
        parser_stats = parser_stats or service_parser_stats
    
    return await process_month_queue_async(
        month=month,
        config=config,
        stop_event=stop_event,
        parser_stats=parser_stats,
        limit=limit
    )
