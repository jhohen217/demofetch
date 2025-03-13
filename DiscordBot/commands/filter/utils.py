"""
Utility functions for the filter module.
"""

import os
import asyncio
import json
from typing import Set, List, Dict, Any, Optional, Tuple
from dataclasses import dataclass
from datetime import datetime

@dataclass
class MatchResult:
    """
    Class to store match filtering results.
    """
    match_id: str
    textfiles_dir: str
    has_ace: bool = False
    has_quad: bool = False
    ace_players: List[str] = None
    quad_players: List[str] = None
    ace_count: int = 0
    quad_count: int = 0
    match_date: Optional[datetime] = None

    def __post_init__(self):
        self.ace_players = self.ace_players or []
        self.quad_players = self.quad_players or []

    @property
    def formatted_match_id(self) -> str:
        """
        Format match ID with date and count prefixes.
        
        Format: MM-DD-YY_XXXX_matchid where:
        - MM-DD-YY is the match date
        - XXXX is ace count and quad count
        - matchid is the original match ID
        
        Returns:
            str: Formatted match ID
        """
        date_prefix = self.match_date.strftime("%m-%d-%y") if self.match_date else "00-00-00"
        count_prefix = f"{self.ace_count:02d}{self.quad_count:02d}"
        return f"{date_prefix}_{count_prefix}_{self.match_id}"

    @property
    def target_file(self) -> str:
        """
        Get the target file path for this match result.
        
        Returns:
            str: Path to the target file
        """
        # Get current month directory and name
        current_month = datetime.now().strftime("%B")  # e.g., "February"
        month_dir = os.path.join(self.textfiles_dir, current_month)
        month_lower = current_month.lower()
        
        if self.has_ace:  # Save to ace_matchids.txt if it has any ace kills
            return os.path.join(month_dir, f"ace_matchids_{month_lower}.txt")
        elif self.has_quad:  # Save to quad_matchids.txt if it has any quad kills
            return os.path.join(month_dir, f"quad_matchids_{month_lower}.txt")
        else:
            return os.path.join(month_dir, f"unapproved_matchids_{month_lower}.txt")

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

def write_to_file_with_flush(filepath: str, match_id: str, formatted: bool = False) -> None:
    """
    Helper function to safely write a match_id to a file.
    
    Args:
        filepath: Path to the file
        match_id: Match ID to write
        formatted: Whether the match ID is formatted (XX_YY_matchid)
    """
    try:
        ensure_file_exists(filepath)
        
        # Read existing content
        existing_lines = []
        if os.path.exists(filepath):
            with open(filepath, "r") as f:
                existing_lines = [line.strip() for line in f if line.strip()]
        
        # Check if match_id already exists
        # For formatted IDs (XX_YY_matchid), compare only the matchid part
        if formatted:
            existing_match_ids = {line.split('_')[-1] for line in existing_lines}
            new_match_id = match_id.split('_')[-1]
            if new_match_id in existing_match_ids:
                print(f"Match {match_id} already exists in {filepath}, skipping...")
                return
        else:
            if match_id in existing_lines:
                print(f"Match {match_id} already exists in {filepath}, skipping...")
                return
        
        # Add new match_id
        existing_lines.append(match_id)
        
        # Always sort ace_file and quad_file (formatted IDs)
        # This ensures the files are always in chronological order
        if "ace_matchids" in filepath or "quad_matchids" in filepath:
            existing_lines.sort()  # Simple alphabetical sort
            print(f"Sorted {os.path.basename(filepath)} in chronological order")
        
        # Write back all content
        with open(filepath, "w") as f:
            for line in existing_lines:
                f.write(line + "\n")
                f.flush()
            os.fsync(f.fileno())  # Force write to disk
    except Exception as e:
        print(f"Error writing to {filepath}: {str(e)}")
        raise

def print_highlighted(message: str) -> None:
    """
    Print a message in a highlighted format.
    
    Args:
        message: Message to print
    """
    print(f"\n{message}")
