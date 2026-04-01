"""
Utility functions for the parser module.
"""

import os
import logging
import asyncio
import aiofiles
from filelock import FileLock
from typing import Set, List, Dict, Tuple, Optional, Counter, Any, Callable

logger = logging.getLogger('discord_bot')

# Set up a separate logger for debug messages that won't be shown in console
debug_logger = logging.getLogger('debug_discord_bot')

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

def extract_short_id(match_id: str) -> str:
    """Extract just the short ID for display"""
    if '_' in match_id:
        parts = match_id.split('_')
        if len(parts) >= 3:
            full_id = parts[2]  # 1-72d8eb18-0bc8-49af-a738-55b792248b79
            # Get the first 8 chars after the "1-" prefix
            return full_id.split('-', 1)[1][:8] if '-' in full_id else full_id[:8]
    elif '-' in match_id:
        # It might be just the match ID part without the date/time prefix
        return match_id.split('-', 1)[1][:8] if '-' in match_id else match_id[:8]
    return match_id[:8]  # Just take the first 8 chars

def format_time_duration(seconds: float) -> str:
    """Format time duration in a human-readable format"""
    if seconds < 60:
        return f"{seconds:.1f} seconds"
    elif seconds < 3600:
        minutes = seconds / 60
        return f"{minutes:.1f} minutes"
    else:
        hours = seconds / 3600
        return f"{hours:.1f} hours"

def read_file_lines(file_path: str) -> Set[str]:
    """Read lines from a file into a set"""
    lines = set()
    if os.path.exists(file_path):
        with open(file_path, 'r', encoding='utf-8') as f:
            lines = {line.strip() for line in f if line.strip()}
    return lines

def write_file_lines(file_path: str, lines: List[str]):
    """Write lines to a file"""
    with open(file_path, 'w', encoding='utf-8') as f:
        for line in lines:
            f.write(f"{line}\n")

def append_file_line(file_path: str, line: str):
    """Append a line to a file"""
    with open(file_path, 'a', encoding='utf-8') as f:
        f.write(f"{line}\n")

async def async_read_file_lines(file_path: str) -> Set[str]:
    """Read lines from a file into a set asynchronously"""
    lines = set()
    if os.path.exists(file_path):
        async with aiofiles.open(file_path, 'r', encoding='utf-8') as f:
            content = await f.read()
            lines = {line.strip() for line in content.splitlines() if line.strip()}
    return lines

async def async_write_file_lines(file_path: str, lines: List[str], use_temp_file: bool = True):
    """Write lines to a file asynchronously with optional atomic update"""
    if use_temp_file:
        # Use a temporary file for atomic update
        temp_path = f"{file_path}.temp"
        async with aiofiles.open(temp_path, 'w', encoding='utf-8') as f:
            for line in lines:
                await f.write(f"{line}\n")
        # Atomic replace
        os.replace(temp_path, file_path)
    else:
        # Direct write
        async with aiofiles.open(file_path, 'w', encoding='utf-8') as f:
            for line in lines:
                await f.write(f"{line}\n")

async def async_append_file_line(file_path: str, line: str):
    """Append a line to a file asynchronously"""
    async with aiofiles.open(file_path, 'a', encoding='utf-8') as f:
        await f.write(f"{line}\n")

async def retry_operation(operation: Callable, max_retries: int = 3, initial_delay: float = 1.0):
    """
    Retry an async operation with exponential backoff
    
    Args:
        operation: Async function to retry
        max_retries: Maximum number of retry attempts
        initial_delay: Initial delay between retries in seconds
        
    Returns:
        The result of the operation if successful
        
    Raises:
        The last exception encountered if all retries fail
    """
    delay = initial_delay
    last_exception = None
    
    for attempt in range(max_retries):
        try:
            return await operation()
        except Exception as e:
            last_exception = e
            if attempt < max_retries - 1:
                logger.warning(f"Operation failed, retrying in {delay}s: {str(e)}")
                await asyncio.sleep(delay)
                delay *= 2  # Exponential backoff
            else:
                logger.error(f"Operation failed after {max_retries} attempts: {str(e)}")
                raise last_exception

def extract_uuid_from_demo_id(demo_id: str) -> str:
    """
    Extract the UUID part from a demo ID
    
    Args:
        demo_id: Demo ID string (e.g., "03-08-25_2029_1-8e335053-1a81-4746-bae7-ef7d2da0525e")
        
    Returns:
        str: The UUID part of the demo ID (e.g., "1-8e335053-1a81-4746-bae7-ef7d2da0525e")
    """
    # First, try to extract using a pattern match for the standard UUID format
    import re
    match = re.search(r'(1-[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})', demo_id)
    if match:
        return match.group(1)
    
    # Check if the demo_id has a prefix (format: MM-DD-YY_HHMM_UUID)
    if '_' in demo_id:
        parts = demo_id.split('_')
        if len(parts) >= 3:
            # Check if the last part is a valid UUID
            if parts[-1].startswith("1-"):
                return parts[-1]  # Return the UUID part
    
    # Check if the demo_id is already in UUID format (starts with "1-")
    if demo_id.startswith("1-"):
        return demo_id
    
    # Log a warning if we couldn't extract a valid UUID
    debug_logger.warning(f"Could not extract valid UUID from demo_id: {demo_id}")
    
    # If all else fails, return the original
    return demo_id

def has_prefix(demo_id: str) -> bool:
    """
    Check if a demo ID has a date/time prefix
    
    Args:
        demo_id: Demo ID string
        
    Returns:
        bool: True if the demo ID has a prefix, False otherwise
    """
    # Check if the ID has the format like "12-07-24_0101_1-a46eb099-3c5d-4443-8b72-4dc1fa5975f4"
    parts = demo_id.split('_')
    if len(parts) >= 3:
        # Check if the first part looks like a date (MM-DD-YY)
        date_part = parts[0]
        if len(date_part) == 8 and date_part[2] == '-' and date_part[5] == '-':
            return True
    return False

async def alphabetize_file(file_path: str, remove_duplicates: bool = True, preserve_chronological: bool = False):
    """
    Alphabetize the lines in a file and optionally remove duplicates based on UUID
    
    Args:
        file_path: Path to the file to alphabetize
        remove_duplicates: Whether to remove duplicate entries with the same UUID
        preserve_chronological: Whether to preserve chronological order for prefixed entries
    """
    if os.path.exists(file_path):
        try:
            # Read all lines from the file
            lines = await async_read_file_lines(file_path)
            
            if remove_duplicates:
                # Keep track of seen UUIDs and their corresponding full match IDs
                seen_uuids = {}  # uuid -> full match ID
                unique_lines = []
                
                # First, sort the lines alphabetically
                sorted_lines = sorted(list(lines))
                
                # Process each line
                for line in sorted_lines:
                    uuid = extract_uuid_from_demo_id(line)
                    
                    if uuid not in seen_uuids:
                        # First time seeing this UUID, add it
                        seen_uuids[uuid] = line
                        unique_lines.append(line)
                    else:
                        # We've seen this UUID before
                        existing_line = seen_uuids[uuid]
                        
                        # If the new line has a prefix and the existing one doesn't,
                        # replace the existing one with the new one
                        if has_prefix(line) and not has_prefix(existing_line):
                            # Remove the existing line
                            unique_lines.remove(existing_line)
                            # Add the new line with prefix
                            unique_lines.append(line)
                            # Update the seen_uuids dictionary
                            seen_uuids[uuid] = line
                            debug_logger.info(f"Replaced {existing_line} with {line} (preferring prefixed version)")
                
                sorted_lines = unique_lines
                debug_logger.info(f"Removed {len(lines) - len(unique_lines)} duplicates from file: {file_path}")
            else:
                # Just use the lines without removing duplicates
                sorted_lines = list(lines)
            
            # Sort the lines based on the preserve_chronological flag
            if preserve_chronological and (os.path.basename(file_path).startswith(('ace_matchids', 'quad_matchids')) or 'parse_queue' in os.path.basename(file_path)):
                # For matchid files and parse queue files, we want to sort by date/time prefix if present
                # This ensures older demos are processed first
                def sort_key(line):
                    if has_prefix(line):
                        # Extract the date/time part for sorting
                        parts = line.split('_')
                        if len(parts) >= 2:
                            # Return the date/time parts for sorting
                            return parts[0] + '_' + parts[1]
                    # For lines without prefix, use the UUID
                    return extract_uuid_from_demo_id(line)
                
                sorted_lines.sort(key=sort_key)
                debug_logger.info(f"Chronologically sorted file: {file_path}")
            else:
                # Standard alphabetical sort
                sorted_lines.sort()
                debug_logger.info(f"Alphabetically sorted file: {file_path}")
            
            # Write the sorted lines back to the file
            await async_write_file_lines(file_path, sorted_lines, use_temp_file=True)
            
        except Exception as e:
            logger.error(f"Error alphabetizing file {file_path}: {str(e)}")

async def create_uuid_only_file(input_file_path: str, output_file_path: str = None, preserve_order: bool = True) -> str:
    """
    Create a temporary file with UUIDs extracted from a file with prefixed matchids
    
    Args:
        input_file_path: Path to the input file with prefixed matchids
        output_file_path: Optional path for the output file. If None, uses input_file_path + '.uuid'
        preserve_order: Whether to preserve the original order of entries
        
    Returns:
        str: Path to the created UUID-only file
    """
    if not os.path.exists(input_file_path):
        logger.warning(f"Input file does not exist: {input_file_path}")
        return None
    
    # If output path not provided, use input path + '.uuid'
    if output_file_path is None:
        output_file_path = f"{input_file_path}.uuid"
    
    try:
        # Read all lines from the input file as a list to preserve order
        with open(input_file_path, 'r', encoding='utf-8') as f:
            lines = [line.strip() for line in f if line.strip()]
        
        # Extract UUIDs from each line
        uuid_lines = [extract_uuid_from_demo_id(line) for line in lines]
        
        # Write UUIDs to the output file
        with open(output_file_path, 'w', encoding='utf-8') as f:
            for line in uuid_lines:
                f.write(f"{line}\n")
        
        logger.info(f"Created UUID-only file: {output_file_path} with {len(uuid_lines)} entries (order preserved: {preserve_order})")
        return output_file_path
        
    except Exception as e:
        logger.error(f"Error creating UUID-only file from {input_file_path}: {str(e)}")
        return None
