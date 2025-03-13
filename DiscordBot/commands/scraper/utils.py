"""
Utility functions for the scraper module.
"""

import os
import asyncio
from typing import Set, List, Optional, Tuple

def ensure_file_exists(filepath: str) -> None:
    """
    Ensure a file exists, create it if it doesn't.
    
    Args:
        filepath: Path to the file
    """
    try:
        if not os.path.exists(filepath):
            # Ensure directory exists
            os.makedirs(os.path.dirname(filepath), exist_ok=True)
            with open(filepath, "w") as f:
                f.write("")  # Create empty file
    except Exception as e:
        print(f"Error creating file {filepath}: {str(e)}")
        raise

async def async_read_file_lines(filepath: str) -> List[str]:
    """
    Read lines from a file asynchronously.
    
    Args:
        filepath: Path to the file
        
    Returns:
        List[str]: List of non-empty lines
    """
    if not os.path.exists(filepath):
        return []
    
    try:
        # Use asyncio.to_thread for file I/O to avoid blocking
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(None, lambda: _read_file_lines(filepath))
    except Exception as e:
        print(f"Error reading file {filepath}: {str(e)}")
        return []

def _read_file_lines(filepath: str) -> List[str]:
    """
    Read lines from a file synchronously.
    
    Args:
        filepath: Path to the file
        
    Returns:
        List[str]: List of non-empty lines
    """
    with open(filepath, "r", encoding="utf-8") as f:
        return [line.strip() for line in f if line.strip()]

def write_to_file_with_flush(filepath: str, content: str) -> None:
    """
    Write content to a file with flush to ensure it's written to disk.
    
    Args:
        filepath: Path to the file
        content: Content to write
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, "w", encoding="utf-8") as f:
            f.write(content)
            f.flush()
            os.fsync(f.fileno())  # Force write to disk
    except Exception as e:
        print(f"Error writing to {filepath}: {str(e)}")
        raise

def append_to_file_with_flush(filepath: str, line: str) -> None:
    """
    Append a line to a file with flush to ensure it's written to disk.
    
    Args:
        filepath: Path to the file
        line: Line to append
    """
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        
        with open(filepath, "a", encoding="utf-8") as f:
            f.write(line + "\n")
            f.flush()
            os.fsync(f.fileno())  # Force write to disk
    except Exception as e:
        print(f"Error appending to {filepath}: {str(e)}")
        raise

def print_highlighted(message: str) -> None:
    """
    Print a message in a highlighted format.
    
    Args:
        message: Message to print
    """
    print(f"\n{message}")
