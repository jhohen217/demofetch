"""
Functions for managing CS2 demo parsing queues.
"""

import os
import logging
import asyncio
import re
from datetime import datetime
from typing import Dict, List, Set, Tuple, Optional

from commands.parser.utils import (
    async_read_file_lines,
    async_write_file_lines,
    retry_operation,
    create_uuid_only_file,
    extract_uuid_from_demo_id
)
from commands.parser.config import get_config

logger = logging.getLogger('discord_bot')
# Set up a separate logger for debug messages that won't be shown in console
debug_logger = logging.getLogger('debug_discord_bot')

async def prepare_parse_queue_async(month: str, config: Dict, stop_event: asyncio.Event = None, limit: int = None) -> Tuple[bool, Dict]:
    """
    Prepare parsing queue for a specific month asynchronously
    
    Args:
        month: Month name (e.g., "February")
        config: Configuration dictionary
        stop_event: Optional event to signal stopping
        
    Returns:
        Tuple[bool, Dict]: Success status and stats
    """
    # Get file paths
    textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
    if not textfiles_dir:
        logger.error("Textfiles directory not found in config")
        return False, {'error': "Textfiles directory not found in config"}
    
    month_dir = os.path.join(textfiles_dir, month)
    month_lower = month.lower()
    
    files = {
        'dir': month_dir,
        'downloaded': os.path.join(month_dir, f'downloaded_{month_lower}.txt'),
        'parsed': os.path.join(month_dir, f'parsed_{month_lower}.txt'),
        'parse_queue': os.path.join(month_dir, f'parse_queue_{month_lower}.txt'),
        'rejected': os.path.join(month_dir, f'rejected_{month_lower}.txt')
    }
    
    # Create rejected file if it doesn't exist
    if not os.path.exists(files['rejected']):
        with open(files['rejected'], 'w', encoding='utf-8') as f:
            pass
    
    # Create month directory if it doesn't exist
    os.makedirs(files['dir'], exist_ok=True)
    
    # Read downloaded, parsed, and rejected demos with retry
    downloaded_demos = await retry_operation(
        lambda: async_read_file_lines(files['downloaded'])
    )
    
    parsed_demos = await retry_operation(
        lambda: async_read_file_lines(files['parsed'])
    )
    
    rejected_demos = await retry_operation(
        lambda: async_read_file_lines(files['rejected'])
    )
    
    # Initialize unprocessed_demos as an empty set
    unprocessed_demos = set()
    
    # We'll only add demos to unprocessed_demos if they're in both downloaded_demos
    # AND in either ace_matchids or quad_matchids files
    
    # Also check ace_matchids file for this month
    ace_matchids_file = os.path.join(month_dir, f'ace_matchids_{month_lower}.txt')
    quad_matchids_file = os.path.join(month_dir, f'quad_matchids_{month_lower}.txt')
    
    # Create sets of UUIDs from downloaded, parsed, and rejected demos
    # These should already be in UUID format, but we'll extract just to be safe
    downloaded_uuids = {extract_uuid_from_demo_id(demo) for demo in downloaded_demos}
    parsed_uuids = {extract_uuid_from_demo_id(demo) for demo in parsed_demos}
    rejected_uuids = {extract_uuid_from_demo_id(demo) for demo in rejected_demos}
    
    # Process ace_matchids file if it exists - using our new UUID-only file approach
    if os.path.exists(ace_matchids_file):
            # Get the reference order from the ace_matchids file
            ace_reference_order = []
            if os.path.exists(ace_matchids_file):
                with open(ace_matchids_file, 'r', encoding='utf-8') as f:
                    ace_matchids = [line.strip() for line in f if line.strip()]
                    # Extract UUIDs while preserving order
                    for matchid in ace_matchids:
                        uuid = extract_uuid_from_demo_id(matchid)
                        ace_reference_order.append(uuid)
            
            # Create a UUID-only version of the ace_matchids file with preserved order
            ace_uuid_file = await create_uuid_only_file(ace_matchids_file, preserve_order=True)
            
            if ace_uuid_file:
                # Read the UUID-only file as a list to preserve order
                with open(ace_uuid_file, 'r', encoding='utf-8') as f:
                    ace_uuids_list = [line.strip() for line in f if line.strip()]
                
                # Convert to set for efficient comparison
                ace_uuids_set = set(ace_uuids_list)
                
                logger.info(f"Processing {len(ace_uuids_list)} entries from ace_matchids file for {month}")
                
                # Find UUIDs that are in downloaded_uuids but not in parsed_uuids or rejected_uuids
                # This ensures we only process demos that have been downloaded
                processable_uuids = downloaded_uuids - parsed_uuids - rejected_uuids
                
                logger.info(f"Found {len(processable_uuids)} processable UUIDs in downloaded_demos for {month}")
                logger.info(f"Found {len(ace_uuids_set)} UUIDs in ace_matchids file for {month}")
                
                # Find UUIDs from ace_matchids that are processable
                new_ace_uuids = ace_uuids_set.intersection(processable_uuids)
                
                logger.info(f"Found {len(new_ace_uuids)} UUIDs that are in both downloaded_demos and ace_matchids for {month}")
                
                # If we found processable UUIDs, add them to unprocessed_demos in their original order
                if new_ace_uuids:
                    # Add the UUIDs to unprocessed_demos in their original order from the reference list
                    for uuid in ace_reference_order:
                        if uuid in new_ace_uuids:
                            unprocessed_demos.add(uuid)
                            debug_logger.info(f"Adding ace matchid UUID {uuid} to unprocessed demos")
                    
                    logger.info(f"Added {len(new_ace_uuids)} processable ace matchids to unprocessed demos")
                
                # Clean up the temporary file
                try:
                    os.remove(ace_uuid_file)
                    debug_logger.info(f"Removed temporary file: {ace_uuid_file}")
                except Exception as e:
                    debug_logger.error(f"Error removing temporary file {ace_uuid_file}: {str(e)}")
    
    # Process quad_matchids file if it exists - using our new UUID-only file approach
    if os.path.exists(quad_matchids_file):
        # Get the reference order from the quad_matchids file
        quad_reference_order = []
        if os.path.exists(quad_matchids_file):
            with open(quad_matchids_file, 'r', encoding='utf-8') as f:
                quad_matchids = [line.strip() for line in f if line.strip()]
                # Extract UUIDs while preserving order
                for matchid in quad_matchids:
                    uuid = extract_uuid_from_demo_id(matchid)
                    quad_reference_order.append(uuid)
        
        # Create a UUID-only version of the quad_matchids file with preserved order
        quad_uuid_file = await create_uuid_only_file(quad_matchids_file, preserve_order=True)
        
        if quad_uuid_file:
            # Read the UUID-only file as a list to preserve order
            with open(quad_uuid_file, 'r', encoding='utf-8') as f:
                quad_uuids_list = [line.strip() for line in f if line.strip()]
            
            # Convert to set for efficient comparison
            quad_uuids_set = set(quad_uuids_list)
            
            logger.info(f"Processing {len(quad_uuids_list)} entries from quad_matchids file for {month}")
            
            # Find UUIDs that are in downloaded_uuids but not in parsed_uuids or rejected_uuids
            # This ensures we only process demos that have been downloaded
            processable_uuids = downloaded_uuids - parsed_uuids - rejected_uuids
            
            logger.info(f"Found {len(processable_uuids)} processable UUIDs in downloaded_demos for {month}")
            logger.info(f"Found {len(quad_uuids_set)} UUIDs in quad_matchids file for {month}")
            
            # Find UUIDs from quad_matchids that are processable
            new_quad_uuids = quad_uuids_set.intersection(processable_uuids)
            
            logger.info(f"Found {len(new_quad_uuids)} UUIDs that are in both downloaded_demos and quad_matchids for {month}")
            
            # If we found processable UUIDs, add them to unprocessed_demos in their original order
            if new_quad_uuids:
                # Add the UUIDs to unprocessed_demos in their original order from the reference list
                for uuid in quad_reference_order:
                    if uuid in new_quad_uuids:
                        unprocessed_demos.add(uuid)
                        debug_logger.info(f"Adding quad matchid UUID {uuid} to unprocessed demos")
                
                logger.info(f"Added {len(new_quad_uuids)} processable quad matchids to unprocessed demos")
            
            # Clean up the temporary file
            try:
                os.remove(quad_uuid_file)
                debug_logger.info(f"Removed temporary file: {quad_uuid_file}")
            except Exception as e:
                debug_logger.error(f"Error removing temporary file {quad_uuid_file}: {str(e)}")
    
    # Log the summary results instead of each individual demo
    logger.info(f"Found {len(unprocessed_demos)} unprocessed demos for {month}")
        
    # Also remove any demos from downloaded_demos that are in rejected_demos
    # This ensures the downloaded file doesn't contain rejected demos
    if rejected_demos:
        demos_to_remove = downloaded_demos.intersection(rejected_demos)
        if demos_to_remove:
            logger.info(f"Found {len(demos_to_remove)} demos in downloaded file that are also in rejected file")
            
            # Update downloaded file to remove rejected demos
            updated_downloaded = list(downloaded_demos - rejected_demos)
            updated_downloaded.sort()  # Sort to maintain order
            
            # Write back to downloaded file
            await retry_operation(
                lambda: async_write_file_lines(files['downloaded'], updated_downloaded, use_temp_file=True)
            )
            
            logger.info(f"Removed {len(demos_to_remove)} rejected demos from downloaded file")
    
    # Read existing queue with retry
    existing_queue = await retry_operation(
        lambda: async_read_file_lines(files['parse_queue'])
    )
    
    # Add new demos to queue
    # If limit is specified, only add up to limit demos
    if limit is not None and limit > 0:
        # Convert to list and sort for consistent results
        unprocessed_list = sorted(list(unprocessed_demos))
        # Only take up to limit demos
        limited_unprocessed = unprocessed_list[:limit]
        logger.info(f"Limiting queue to {limit} demos (from {len(unprocessed_demos)} available)")
        new_queue = list(existing_queue.union(limited_unprocessed))
    else:
        new_queue = list(existing_queue.union(unprocessed_demos))
    
    # Initialize stats dictionary
    stats = {
        'total_downloaded': len(downloaded_demos),
        'already_parsed': len(parsed_demos),
        'unprocessed': len(unprocessed_demos),
        'unarchived_demos': 0,
        'queue_size': len(new_queue)
    }
    
    # Check if stop event is set before scanning for unarchived demos
    if stop_event and stop_event.is_set():
        logger.info("Stop event detected, skipping unarchived demo scanning")
        return True, stats
    
    # Scan for unarchived .dem files in the demos directory
    demos_dir = config.get('project', {}).get('public_demos_directory', '')
    
    if demos_dir:
        # Check both the month-specific directory and the root demos directory
        month_dir = os.path.join(demos_dir, month)
        unarchived_demos = []
        
        # Function to extract match ID from filename
        def extract_match_id_from_filename(filename):
            # Remove .dem extension
            base_name = filename.replace('.dem', '')
            
            # Check if it's already in the format we expect (match ID)
            if base_name.startswith('1-'):
                return base_name
            
            # Try to extract match ID using regex
            match = re.search(r'1-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', base_name)
            if match:
                return match.group(0)
            
            # If we can't extract a match ID, return None
            return None
        
        # Function to check if a .dem file has a corresponding .dem.gz file
        def has_archived_version(dem_path):
            gz_path = dem_path + '.gz'
            return os.path.exists(gz_path)
        
        # Scan month directory
        if os.path.exists(month_dir):
            try:
                for filename in os.listdir(month_dir):
                    if filename.endswith('.dem') and not filename.endswith('.dem.gz'):
                        dem_path = os.path.join(month_dir, filename)
                        
                        # Only process .dem files that don't have a corresponding .dem.gz file
                        if not has_archived_version(dem_path):
                            match_id = extract_match_id_from_filename(filename)
                            if match_id:
                                # Extract UUID and add it directly without prefix
                                demo_uuid = extract_uuid_from_demo_id(match_id)
                                unarchived_demos.append(demo_uuid)
                                debug_logger.info(f"Found unarchived demo: {dem_path}, adding UUID {demo_uuid} to queue")
            except PermissionError as e:
                # Handle access denied errors (e.g., for System Volume Information directory)
                logger.warning(f"Permission denied when scanning directory {month_dir}: {str(e)}")
            except Exception as e:
                logger.error(f"Error scanning directory {month_dir}: {str(e)}")
        
        # Also scan the root demos directory
        try:
            if os.path.exists(demos_dir):
                for filename in os.listdir(demos_dir):
                    if filename.endswith('.dem') and not filename.endswith('.dem.gz'):
                        dem_path = os.path.join(demos_dir, filename)
                        
                        # Only process .dem files that don't have a corresponding .dem.gz file
                        if not has_archived_version(dem_path):
                            match_id = extract_match_id_from_filename(filename)
                            if match_id:
                                # Extract UUID and add it directly without prefix
                                demo_uuid = extract_uuid_from_demo_id(match_id)
                                unarchived_demos.append(demo_uuid)
                                debug_logger.info(f"Found unarchived demo in root directory: {dem_path}, adding UUID {demo_uuid} to queue")
        except PermissionError as e:
            # Handle access denied errors (e.g., for System Volume Information directory)
            logger.warning(f"Permission denied when scanning root demos directory {demos_dir}: {str(e)}")
        except Exception as e:
            logger.error(f"Error scanning root demos directory {demos_dir}: {str(e)}")
            
        # We already imported extract_uuid_from_demo_id at the top of the file
        
        # Create sets of UUIDs from parsed and rejected demos for unarchived demo filtering
        parsed_uuids = {demo for demo in parsed_demos}
        rejected_uuids = {demo for demo in rejected_demos}
        
        # Get UUIDs from ace_matchids and quad_matchids files
        ace_uuids_set = set()
        quad_uuids_set = set()
        
        if os.path.exists(ace_matchids_file):
            with open(ace_matchids_file, 'r', encoding='utf-8') as f:
                ace_matchids_list = [line.strip() for line in f if line.strip()]
                ace_uuids_set = {extract_uuid_from_demo_id(matchid) for matchid in ace_matchids_list}
        
        if os.path.exists(quad_matchids_file):
            with open(quad_matchids_file, 'r', encoding='utf-8') as f:
                quad_matchids_list = [line.strip() for line in f if line.strip()]
                quad_uuids_set = {extract_uuid_from_demo_id(matchid) for matchid in quad_matchids_list}
        
        # Combine ace and quad UUIDs
        matchids_uuids_set = ace_uuids_set.union(quad_uuids_set)
        
        # Filter unarchived demos to remove those that have already been parsed or rejected
        # or are not in the ace_matchids or quad_matchids files
        filtered_unarchived_demos = []
        skipped_parsed_count = 0
        skipped_rejected_count = 0
        skipped_not_in_matchids_count = 0
        
        for demo in unarchived_demos:
            demo_uuid = extract_uuid_from_demo_id(demo)
            if demo_uuid in parsed_uuids:
                skipped_parsed_count += 1
                debug_logger.info(f"Skipping already parsed unarchived demo (UUID match): {demo}")
            elif demo_uuid in rejected_uuids:
                skipped_rejected_count += 1
                debug_logger.info(f"Skipping previously rejected unarchived demo (UUID match): {demo}")
            elif demo_uuid not in matchids_uuids_set:
                skipped_not_in_matchids_count += 1
                debug_logger.info(f"Skipping unarchived demo not in matchids files: {demo}")
            else:
                # Add the UUID without prefix to the filtered list
                filtered_unarchived_demos.append(demo_uuid)
        
        # Log summary of skipped demos
        if skipped_parsed_count > 0:
            logger.info(f"Skipped {skipped_parsed_count} already parsed unarchived demos")
        if skipped_rejected_count > 0:
            logger.info(f"Skipped {skipped_rejected_count} previously rejected unarchived demos")
        if skipped_not_in_matchids_count > 0:
            logger.info(f"Skipped {skipped_not_in_matchids_count} unarchived demos not in matchids files")
        
        # Add filtered unarchived demos to the queue
        if filtered_unarchived_demos:
            logger.info(f"Adding {len(filtered_unarchived_demos)} unarchived demos to the queue for {month}")
            new_queue = list(set(new_queue).union(set(filtered_unarchived_demos)))
    
    # Use the alphabetize_file function to sort and remove duplicates
    # First write the queue to the file
    await retry_operation(
        lambda: async_write_file_lines(files['parse_queue'], new_queue, use_temp_file=True)
    )
    
    # Then alphabetize and remove duplicates
    from commands.parser.utils import alphabetize_file
    await alphabetize_file(files['parse_queue'], remove_duplicates=True, preserve_chronological=True)
    
    # Read the updated queue back
    new_queue = list(await retry_operation(
        lambda: async_read_file_lines(files['parse_queue'])
    ))
    
    logger.info(f"Alphabetized parse queue and removed duplicates for {month}")
    
    stats = {
        'total_downloaded': len(downloaded_demos),
        'already_parsed': len(parsed_demos),
        'unprocessed': len(unprocessed_demos),
        'unarchived_demos': len(unarchived_demos) if 'unarchived_demos' in locals() else 0,
        'queue_size': len(new_queue)
    }
    
    return True, stats

async def prepare_parse_queue(month: str, stop_event: asyncio.Event = None, limit: int = None) -> Tuple[bool, Dict]:
    """
    Prepare parsing queue for a specific month (wrapper for async version)
    
    Args:
        month: Month name (e.g., "February")
        stop_event: Optional event to signal stopping
        limit: Maximum number of demos to add to the queue
        
    Returns:
        Tuple[bool, Dict]: Success status and stats
    """
    config = get_config()
    return await prepare_parse_queue_async(month, config, stop_event, limit)
