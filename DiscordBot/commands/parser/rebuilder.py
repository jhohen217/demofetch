"""
Functions for rebuilding parsed and downloaded files by scanning the appropriate directories.
"""

import os
import glob
import logging
from typing import Tuple, Set, List, Dict

from commands.parser.config import get_config
from commands.parser.utils import read_file_lines, write_file_lines, alphabetize_file, extract_uuid_from_demo_id

logger = logging.getLogger('discord_bot')
debug_logger = logging.getLogger('debug_discord_bot')

async def rebuild_parsed_file(month: str) -> Tuple[bool, str]:
    """
    Rebuild the parsed_[month].txt file by scanning the KillCollections directory
    
    Args:
        month: Month name (e.g., "February")
        
    Returns:
        Tuple[bool, str]: Success status and message
    """
    try:
        # Get configuration
        config = get_config()
        kill_collection_path = config.get('project', {}).get('KillCollectionParse', '')
        textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
        
        if not kill_collection_path or not textfiles_dir:
            return False, "Error: Could not find KillCollectionParse or textfiles directory in config"
        
        # Get month-specific paths
        month_dir = os.path.join(textfiles_dir, month)
        month_lower = month.lower()
        parsed_file = os.path.join(month_dir, f'parsed_{month_lower}.txt')
        
        # Create month directory if it doesn't exist
        os.makedirs(month_dir, exist_ok=True)
        
        # Get the kill collection directory for this month
        kill_collection_month_dir = os.path.join(kill_collection_path, month)
        
        # If the kill collection directory doesn't exist, create an empty parsed file
        if not os.path.exists(kill_collection_month_dir):
            logger.info(f"Kill collection directory for {month} not found. Creating empty parsed file.")
            with open(parsed_file, 'w', encoding='utf-8') as f:
                pass  # Create an empty file
            return True, f"Created empty parsed_{month_lower}.txt (no kill collections found)"
        
        # Find all collection files in the month directory
        collection_files = glob.glob(os.path.join(kill_collection_month_dir, "*_col.csv"))
        
        # If no collection files found, create an empty parsed file
        if not collection_files:
            logger.info(f"No collection files found for {month}. Creating empty parsed file.")
            with open(parsed_file, 'w', encoding='utf-8') as f:
                pass  # Create an empty file
            return True, f"Created empty parsed_{month_lower}.txt (no collection files found)"
        
        # Extract demo IDs from filenames
        demo_ids = set()
        for file_path in collection_files:
            filename = os.path.basename(file_path)
            # Extract everything before the first underscore
            if '_' in filename:
                demo_id = filename.split('_', 1)[0]
                demo_ids.add(demo_id)
        
        # Write to parsed file
        with open(parsed_file, 'w', encoding='utf-8') as f:
            for demo_id in sorted(demo_ids):
                f.write(f"{demo_id}\n")
        
        return True, f"Successfully rebuilt parsed_{month_lower}.txt with {len(demo_ids)} demo IDs"
    
    except Exception as e:
        logger.error(f"Error rebuilding parsed file for {month}: {str(e)}")
        return False, f"Error rebuilding parsed file: {str(e)}"

async def rebuild_downloaded_file(month: str) -> Tuple[bool, str]:
    """
    Rebuild the downloaded_{month}.txt file by scanning the demos directory
    
    Args:
        month: Month name (e.g., "February")
        
    Returns:
        Tuple[bool, str]: Success status and message
    """
    try:
        # Get configuration
        config = get_config()
        textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
        demos_dir = config.get('project', {}).get('public_demos_directory', '')
        
        if not textfiles_dir or not demos_dir:
            return False, "Error: Could not find textfiles or public_demos directory in config"
        
        # Get month-specific paths
        month_dir = os.path.join(textfiles_dir, month)
        month_lower = month.lower()
        downloaded_file = os.path.join(month_dir, f'downloaded_{month_lower}.txt')
        
        # Create month directory if it doesn't exist
        os.makedirs(month_dir, exist_ok=True)
        
        # Get the demos directory for this month
        demos_month_dir = os.path.join(demos_dir, month)
        
        # If the demos directory doesn't exist, create an empty downloaded file
        if not os.path.exists(demos_month_dir):
            logger.info(f"Demos directory for {month} not found. Creating empty downloaded file.")
            with open(downloaded_file, 'w', encoding='utf-8') as f:
                pass  # Create an empty file
            return True, f"Created empty downloaded_{month_lower}.txt (no demos found)"
        
        # Find all demo files in the month directory
        demo_files = glob.glob(os.path.join(demos_month_dir, "*.dem")) + glob.glob(os.path.join(demos_month_dir, "*.dem.gz"))
        
        # If no demo files found, create an empty downloaded file
        if not demo_files:
            logger.info(f"No demo files found for {month}. Creating empty downloaded file.")
            with open(downloaded_file, 'w', encoding='utf-8') as f:
                pass  # Create an empty file
            return True, f"Created empty downloaded_{month_lower}.txt (no demo files found)"
        
        # Read existing downloaded file if it exists
        existing_ids = set()
        if os.path.exists(downloaded_file):
            existing_ids = read_file_lines(downloaded_file)
        
        # Extract demo IDs from filenames
        demo_ids = set()
        for file_path in demo_files:
            filename = os.path.basename(file_path)
            # Remove the extension (.dem or .dem.gz)
            if filename.endswith('.dem.gz'):
                demo_id = filename[:-7]  # Remove .dem.gz
            elif filename.endswith('.dem'):
                demo_id = filename[:-4]  # Remove .dem
            else:
                continue
            
            # Check if this is a match ID (UUID format)
            if '-' in demo_id:
                # This is likely a match ID in UUID format
                # Format it as expected in the downloaded file (with prefix if needed)
                # For now, we'll just add it as is
                demo_ids.add(demo_id)
        
        # Combine existing IDs with new ones
        all_ids = existing_ids.union(demo_ids)
        
        # Write to downloaded file
        with open(downloaded_file, 'w', encoding='utf-8') as f:
            for demo_id in sorted(all_ids):
                f.write(f"{demo_id}\n")
        
        # Remove duplicates by UUID
        await alphabetize_file(downloaded_file, remove_duplicates=True)
        
        # Count how many new IDs were added
        new_count = len(demo_ids - existing_ids)
        
        return True, f"Successfully rebuilt downloaded_{month_lower}.txt with {len(all_ids)} demo IDs ({new_count} new)"
    
    except Exception as e:
        logger.error(f"Error rebuilding downloaded file for {month}: {str(e)}")
        return False, f"Error rebuilding downloaded file: {str(e)}"

async def rebuild_all_downloaded_files() -> Tuple[bool, str]:
    """
    Rebuild downloaded files for all months by scanning the demos directory
    
    Returns:
        Tuple[bool, str]: Success status and message
    """
    try:
        # Get configuration
        config = get_config()
        textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
        demos_dir = config.get('project', {}).get('public_demos_directory', '')
        
        if not textfiles_dir or not demos_dir:
            return False, "Error: Could not find textfiles or public_demos directory in config"
        
        # Get all month directories from both textfiles and demos
        textfile_months = set()
        demos_months = set()
        
        # Get months from textfiles directory
        if os.path.exists(textfiles_dir):
            textfile_months = {d for d in os.listdir(textfiles_dir) 
                              if os.path.isdir(os.path.join(textfiles_dir, d)) 
                              and d not in ['undated', 'MergeMe']}
        
        # Get months from demos directory
        if os.path.exists(demos_dir):
            demos_months = {d for d in os.listdir(demos_dir) 
                           if os.path.isdir(os.path.join(demos_dir, d))}
        
        # Combine both sets to get all months
        all_months = textfile_months.union(demos_months)
        
        if not all_months:
            return False, "No month directories found in either textfiles or demos"
        
        # Rebuild downloaded file for each month
        results = []
        for month in sorted(all_months):
            success, message = await rebuild_downloaded_file(month)
            results.append(f"{month}: {'Success' if success else 'Failed'} - {message}")
        
        return True, f"Rebuilt downloaded files for {len(all_months)} months:\n" + "\n".join(results)
    
    except Exception as e:
        logger.error(f"Error rebuilding all downloaded files: {str(e)}")
        return False, f"Error rebuilding all downloaded files: {str(e)}"

async def rebuild_all_parsed_files() -> Tuple[bool, str]:
    """
    Rebuild parsed files for all months by scanning the KillCollections directory
    
    Returns:
        Tuple[bool, str]: Success status and message
    """
    try:
        # Get configuration
        config = get_config()
        kill_collection_path = config.get('project', {}).get('KillCollectionParse', '')
        textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
        
        if not kill_collection_path or not textfiles_dir:
            return False, "Error: Could not find KillCollectionParse or textfiles directory in config"
        
        # Get all month directories from both textfiles and KillCollections
        textfile_months = set()
        kill_collection_months = set()
        
        # Get months from textfiles directory
        if os.path.exists(textfiles_dir):
            textfile_months = {d for d in os.listdir(textfiles_dir) 
                              if os.path.isdir(os.path.join(textfiles_dir, d)) 
                              and d not in ['undated', 'MergeMe']}
        
        # Get months from kill collection path
        if os.path.exists(kill_collection_path):
            kill_collection_months = {d for d in os.listdir(kill_collection_path) 
                                     if os.path.isdir(os.path.join(kill_collection_path, d))}
        
        # Combine both sets to get all months
        all_months = textfile_months.union(kill_collection_months)
        
        if not all_months:
            return False, "No month directories found in either textfiles or KillCollections"
        
        # Rebuild parsed file for each month
        results = []
        for month in sorted(all_months):
            success, message = await rebuild_parsed_file(month)
            results.append(f"{month}: {'Success' if success else 'Failed'} - {message}")
        
        return True, f"Rebuilt parsed files for {len(all_months)} months:\n" + "\n".join(results)
    
    except Exception as e:
        logger.error(f"Error rebuilding all parsed files: {str(e)}")
        return False, f"Error rebuilding all parsed files: {str(e)}"

async def get_reference_order(month: str) -> List[str]:
    """
    Get a reference order of UUIDs from ace_matchids and quad_matchids files
    
    Args:
        month: Month name (e.g., "February")
        
    Returns:
        List[str]: Ordered list of UUIDs
    """
    config = get_config()
    textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
    
    month_dir = os.path.join(textfiles_dir, month)
    month_lower = month.lower()
    
    ace_file = os.path.join(month_dir, f'ace_matchids_{month_lower}.txt')
    quad_file = os.path.join(month_dir, f'quad_matchids_{month_lower}.txt')
    
    reference_order = []
    seen_uuids = set()
    
    # Process ace_matchids file first (higher priority)
    if os.path.exists(ace_file):
        with open(ace_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    uuid = extract_uuid_from_demo_id(line)
                    if uuid not in seen_uuids:
                        reference_order.append(uuid)
                        seen_uuids.add(uuid)
    
    # Then process quad_matchids file
    if os.path.exists(quad_file):
        with open(quad_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    uuid = extract_uuid_from_demo_id(line)
                    if uuid not in seen_uuids:
                        reference_order.append(uuid)
                        seen_uuids.add(uuid)
    
    logger.info(f"Created reference order with {len(reference_order)} UUIDs for {month}")
    return reference_order

async def sort_file_by_reference(file_path: str, reference_order: List[str], remove_duplicates: bool = True):
    """
    Sort a file based on a reference order of UUIDs
    
    Args:
        file_path: Path to the file to sort
        reference_order: List of UUIDs in the desired order
        remove_duplicates: Whether to remove duplicate entries
    """
    if not os.path.exists(file_path):
        logger.warning(f"File does not exist for sorting: {file_path}")
        return
    
    try:
        # Read all lines from the file
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()]
        
        # Create a mapping of UUIDs to lines
        uuid_to_line = {}
        for line in lines:
            uuid = extract_uuid_from_demo_id(line)
            uuid_to_line[uuid] = line
        
        # Create a new sorted list based on reference order
        sorted_lines = []
        for uuid in reference_order:
            if uuid in uuid_to_line:
                sorted_lines.append(uuid_to_line[uuid])
                if remove_duplicates:
                    # If we want to remove duplicates, mark this UUID as used
                    uuid_to_line.pop(uuid)
        
        # Add any remaining lines that weren't in the reference order
        if uuid_to_line:
            for line in sorted(uuid_to_line.values()):
                sorted_lines.append(line)
        
        # Write the sorted lines back to the file
        with open(file_path, 'w', encoding='utf-8') as f:
            for line in sorted_lines:
                f.write(f"{line}\n")
        
        logger.info(f"Sorted file by reference order: {file_path}")
    
    except Exception as e:
        logger.error(f"Error sorting file by reference: {str(e)}")

async def handle_rejected_demos(month: str, reference_order: List[str] = None):
    """
    Handle rejected demos by removing them from all relevant files and folders
    
    Args:
        month: Month name (e.g., "February")
        reference_order: Optional list of UUIDs in reference order
    """
    config = get_config()
    textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
    demos_dir = config.get('project', {}).get('public_demos_directory', '')
    
    month_dir = os.path.join(textfiles_dir, month)
    month_lower = month.lower()
    
    rejected_file = os.path.join(month_dir, f'rejected_{month_lower}.txt')
    parsed_file = os.path.join(month_dir, f'parsed_{month_lower}.txt')
    downloaded_file = os.path.join(month_dir, f'downloaded_{month_lower}.txt')
    parse_queue_file = os.path.join(month_dir, f'parse_queue_{month_lower}.txt')
    ace_file = os.path.join(month_dir, f'ace_matchids_{month_lower}.txt')
    quad_file = os.path.join(month_dir, f'quad_matchids_{month_lower}.txt')
    
    # Read rejected UUIDs
    rejected_uuids = set()
    if os.path.exists(rejected_file):
        with open(rejected_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if line:
                    uuid = extract_uuid_from_demo_id(line)
                    rejected_uuids.add(uuid)
    
    if not rejected_uuids:
        logger.info(f"No rejected demos found for {month}")
        return
    
    logger.info(f"Found {len(rejected_uuids)} rejected UUIDs for {month}")
    
    # Remove rejected UUIDs from parsed file
    if os.path.exists(parsed_file):
        with open(parsed_file, 'r', encoding='utf-8') as f:
            parsed_lines = [line.strip() for line in f if line.strip()]
        
        filtered_lines = []
        for line in parsed_lines:
            uuid = extract_uuid_from_demo_id(line)
            if uuid not in rejected_uuids:
                filtered_lines.append(line)
        
        with open(parsed_file, 'w', encoding='utf-8') as f:
            for line in filtered_lines:
                f.write(f"{line}\n")
        
        logger.info(f"Removed {len(parsed_lines) - len(filtered_lines)} rejected UUIDs from parsed file")
    
    # Remove rejected UUIDs from downloaded file
    if os.path.exists(downloaded_file):
        with open(downloaded_file, 'r', encoding='utf-8') as f:
            downloaded_lines = [line.strip() for line in f if line.strip()]
        
        filtered_lines = []
        for line in downloaded_lines:
            uuid = extract_uuid_from_demo_id(line)
            if uuid not in rejected_uuids:
                filtered_lines.append(line)
        
        with open(downloaded_file, 'w', encoding='utf-8') as f:
            for line in filtered_lines:
                f.write(f"{line}\n")
        
        logger.info(f"Removed {len(downloaded_lines) - len(filtered_lines)} rejected UUIDs from downloaded file")
    
    # Remove rejected UUIDs from parse queue file
    if os.path.exists(parse_queue_file):
        with open(parse_queue_file, 'r', encoding='utf-8') as f:
            queue_lines = [line.strip() for line in f if line.strip()]
        
        filtered_lines = []
        for line in queue_lines:
            uuid = extract_uuid_from_demo_id(line)
            if uuid not in rejected_uuids:
                filtered_lines.append(line)
        
        with open(parse_queue_file, 'w', encoding='utf-8') as f:
            for line in filtered_lines:
                f.write(f"{line}\n")
        
        logger.info(f"Removed {len(queue_lines) - len(filtered_lines)} rejected UUIDs from parse queue file")
    
    # Remove rejected UUIDs from ace_matchids file
    if os.path.exists(ace_file):
        with open(ace_file, 'r', encoding='utf-8') as f:
            ace_lines = [line.strip() for line in f if line.strip()]
        
        filtered_lines = []
        for line in ace_lines:
            uuid = extract_uuid_from_demo_id(line)
            if uuid not in rejected_uuids:
                filtered_lines.append(line)
        
        with open(ace_file, 'w', encoding='utf-8') as f:
            for line in filtered_lines:
                f.write(f"{line}\n")
        
        logger.info(f"Removed {len(ace_lines) - len(filtered_lines)} rejected UUIDs from ace_matchids file")
    
    # Remove rejected UUIDs from quad_matchids file
    if os.path.exists(quad_file):
        with open(quad_file, 'r', encoding='utf-8') as f:
            quad_lines = [line.strip() for line in f if line.strip()]
        
        filtered_lines = []
        for line in quad_lines:
            uuid = extract_uuid_from_demo_id(line)
            if uuid not in rejected_uuids:
                filtered_lines.append(line)
        
        with open(quad_file, 'w', encoding='utf-8') as f:
            for line in filtered_lines:
                f.write(f"{line}\n")
        
        logger.info(f"Removed {len(quad_lines) - len(filtered_lines)} rejected UUIDs from quad_matchids file")
    
    # Delete rejected demo files
    demos_month_dir = os.path.join(demos_dir, month)
    if os.path.exists(demos_month_dir):
        removed_count = 0
        for uuid in rejected_uuids:
            # Try to find and delete the demo file
            for ext in ['.dem', '.dem.gz']:
                demo_path = os.path.join(demos_month_dir, f"{uuid}{ext}")
                if os.path.exists(demo_path):
                    try:
                        os.remove(demo_path)
                        removed_count += 1
                        logger.info(f"Deleted rejected demo file: {demo_path}")
                    except Exception as e:
                        logger.error(f"Error deleting demo file {demo_path}: {str(e)}")
        
        logger.info(f"Removed {removed_count} rejected demo files from demos directory")

async def rebuild_files(month: str) -> Tuple[bool, str]:
    """
    Rebuild both parsed and downloaded files for a specific month while preserving chronological order
    
    Args:
        month: Month name (e.g., "February")
        
    Returns:
        Tuple[bool, str]: Success status and message
    """
    try:
        # Get configuration
        config = get_config()
        textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
        demos_dir = config.get('project', {}).get('public_demos_directory', '')
        kill_collection_path = config.get('project', {}).get('KillCollectionParse', '')
        
        if not textfiles_dir or not demos_dir or not kill_collection_path:
            return False, "Error: Could not find required directories in config"
        
        # Get month-specific paths
        month_dir = os.path.join(textfiles_dir, month)
        month_lower = month.lower()
        
        logger.info(f"Starting rebuild for {month}")
        
        # Get reference order from ace_matchids and quad_matchids
        reference_order = await get_reference_order(month)
        
        if not reference_order:
            logger.warning(f"No reference order found for {month}. Will use alphabetical order.")
        
        # Rebuild parsed file
        parsed_success, parsed_message = await rebuild_parsed_file(month)
        
        # Rebuild downloaded file
        downloaded_success, downloaded_message = await rebuild_downloaded_file(month)
        
        # Sort files based on reference order if available
        parsed_file = os.path.join(month_dir, f'parsed_{month_lower}.txt')
        downloaded_file = os.path.join(month_dir, f'downloaded_{month_lower}.txt')
        
        if reference_order:
            if os.path.exists(parsed_file):
                await sort_file_by_reference(parsed_file, reference_order)
            
            if os.path.exists(downloaded_file):
                await sort_file_by_reference(downloaded_file, reference_order)
        
        # Handle rejected demos
        await handle_rejected_demos(month, reference_order)
        
        # Clean up parse queue file too
        parse_queue_file = os.path.join(month_dir, f'parse_queue_{month_lower}.txt')
        if os.path.exists(parse_queue_file):
            # Read all demos from the file
            with open(parse_queue_file, 'r', encoding='utf-8') as f:
                queue_lines = [line.strip() for line in f if line.strip()]
            
            # Clear the file since we're rebuilding
            with open(parse_queue_file, 'w', encoding='utf-8') as f:
                pass
            
            logger.info(f"Cleared parse queue file for {month}")
        
        success = parsed_success and downloaded_success
        
        return success, f"Successfully rebuilt files for {month}:\n{parsed_message}\n{downloaded_message}"
    
    except Exception as e:
        logger.error(f"Error rebuilding files for {month}: {str(e)}")
        return False, f"Error rebuilding files: {str(e)}"

async def rebuild_all_files() -> Tuple[bool, str]:
    """
    Rebuild both parsed and downloaded files for all months
    
    Returns:
        Tuple[bool, str]: Success status and message
    """
    try:
        # Get configuration
        config = get_config()
        textfiles_dir = config.get('project', {}).get('textfiles_directory', '')
        demos_dir = config.get('project', {}).get('public_demos_directory', '')
        kill_collection_path = config.get('project', {}).get('KillCollectionParse', '')
        
        if not textfiles_dir or not demos_dir or not kill_collection_path:
            return False, "Error: Could not find required directories in config"
        
        # Get all month directories from both textfiles, demos, and KillCollections
        textfile_months = set()
        demos_months = set()
        kill_collection_months = set()
        
        # Get months from textfiles directory
        if os.path.exists(textfiles_dir):
            textfile_months = {d for d in os.listdir(textfiles_dir) 
                              if os.path.isdir(os.path.join(textfiles_dir, d)) 
                              and d not in ['undated', 'MergeMe', 'System Volume Information']}
        
        # Get months from demos directory
        if os.path.exists(demos_dir):
            demos_months = {d for d in os.listdir(demos_dir) 
                           if os.path.isdir(os.path.join(demos_dir, d))
                           and d not in ['System Volume Information']}
        
        # Get months from kill collection path
        if os.path.exists(kill_collection_path):
            kill_collection_months = {d for d in os.listdir(kill_collection_path) 
                                     if os.path.isdir(os.path.join(kill_collection_path, d))
                                     and d not in ['System Volume Information']}
        
        # Combine all sets to get all months
        all_months = textfile_months.union(demos_months).union(kill_collection_months)
        
        if not all_months:
            return False, "No month directories found in textfiles, demos, or KillCollections"
        
        # Rebuild files for each month
        results = []
        for month in sorted(all_months):
            success, message = await rebuild_files(month)
            results.append(f"{month}: {'Success' if success else 'Failed'} - {message}")
        
        return True, f"Rebuilt files for {len(all_months)} months:\n" + "\n".join(results)
    
    except Exception as e:
        logger.error(f"Error rebuilding all files: {str(e)}")
        return False, f"Error rebuilding all files: {str(e)}"
