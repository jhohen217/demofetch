import os
import json
import time
import asyncio
import aiohttp
import logging
from datetime import datetime
from typing import Optional, List, Dict, Tuple, Set
from dataclasses import dataclass

def print_highlighted(message: str):
    """Print a message in a highlighted format"""
    print(f"\n{message}")

@dataclass
class MatchDate:
    match_id: str
    original_name: str
    date: datetime
    month_folder: str
    new_name: str

class DateFetcher:
    def __init__(self):
        # Load configuration from project root
        script_dir = os.path.dirname(os.path.abspath(__file__))  # undated directory
        textfiles_dir = os.path.dirname(script_dir)  # textfiles directory
        project_dir = os.path.dirname(os.path.dirname(textfiles_dir))  # project root
        config_path = os.path.join(project_dir, 'config.json')
        
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        # Set up directories
        self.project_dir = project_dir
        self.api_key = self.config["faceit"]["api_key"]
        self.textfiles_dir = textfiles_dir
        self.undated_dir = script_dir
        
        # Set up processed file path
        self.processed_file = os.path.join(self.undated_dir, "processed.txt")
        
        # API configuration
        self.api_base_url = "https://open.faceit.com/data/v4/matches/{match_id}"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }

        # Rate limiting parameters
        self.rate_limit_delay = 2.0  # 3 seconds between requests
        self.max_retries = 3
        self.rate_limit_cooldown = 60  # 1 minute cooldown if rate limited

        # Statistics
        self.stats = {
            'total': 0,
            'processed': 0,
            'failed': 0,
            'skipped': 0,
            'january': 0,
            'december': 0
        }

        # Cache for processed matches
        self._processed_matches_cache = None
        self._already_processed_cache = None

    def _load_processed_matches(self) -> Dict[str, str]:
        """Load all processed matches from both months into cache
        Returns: Dict[match_id, month]
        """
        if self._processed_matches_cache is not None:
            return self._processed_matches_cache

        processed = {}  # {match_id: month}
        for month in ['January', 'December']:
            month_dir = os.path.join(self.textfiles_dir, month)
            if not os.path.exists(month_dir):
                continue

            for file_type in ['ace', 'quad']:
                file_path = os.path.join(month_dir, f"{file_type}_matchids_{month.lower()}.txt")
                if not os.path.exists(file_path):
                    continue

                with open(file_path, 'r') as f:
                    for line in f:
                        line = line.strip()
                        if line:
                            # Extract match ID from dated name (MM-DD-YY_original_name)
                            parts = line.split('_', 1)
                            if len(parts) > 1:
                                # If it has a date prefix, get the original match ID
                                match_id = self.extract_match_id(parts[1])[1]
                            else:
                                match_id = line
                            processed[match_id] = month

        self._processed_matches_cache = processed
        return processed

    def _load_already_processed(self) -> Set[str]:
        """Load set of already processed match IDs from processed.txt"""
        if self._already_processed_cache is not None:
            return self._already_processed_cache

        processed = set()
        if os.path.exists(self.processed_file):
            with open(self.processed_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    if line:
                        processed.add(line)

        self._already_processed_cache = processed
        return processed

    def _add_to_processed(self, match_id: str):
        """Add a match ID to processed.txt"""
        with open(self.processed_file, 'a') as f:
            f.write(f"{match_id}\n")
        # Update cache
        if self._already_processed_cache is not None:
            self._already_processed_cache.add(match_id)

    def is_match_already_processed(self, match_id: str) -> Tuple[bool, Optional[str]]:
        """Check if a match is already processed
        Returns: (is_processed, month_if_processed)
        """
        # First check if we've processed it before
        already_processed = self._load_already_processed()
        if match_id in already_processed:
            # Then check which month it's in
            processed = self._load_processed_matches()
            if match_id in processed:
                return True, processed[match_id]
            return True, "Unknown"  # Processed but month not found
        return False, None

    def ensure_month_directories(self):
        """Ensure January and December directories exist"""
        for month in ['January', 'December']:
            month_dir = os.path.join(self.textfiles_dir, month)
            os.makedirs(month_dir, exist_ok=True)

    def extract_match_id(self, filename: str) -> tuple[str, str]:
        """Extract match ID from filename with extra info"""
        # Example: 0303_1-ecc4a69a-81b4-42d7-9dfb-3939eee9e979
        # Returns: (original_name, match_id)
        parts = filename.strip().split('_', 1)
        if len(parts) > 1:
            return filename, parts[1]  # Return both original name and match ID
        return filename, filename  # If no extra info, return same string for both

    async def fetch_match_date(self, session: aiohttp.ClientSession, match_id: str) -> Optional[datetime]:
        """Fetch the match date from FACEIT API"""
        url = self.api_base_url.format(match_id=match_id)
        retries = 0
        
        while retries < self.max_retries:
            try:
                # Add delay before making request
                await asyncio.sleep(self.rate_limit_delay)
                
                async with session.get(url, headers=self.headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        # Look for finished_at or started_at in the response
                        timestamp = data.get('finished_at', data.get('started_at'))
                        if timestamp:
                            return datetime.fromtimestamp(timestamp)
                    elif response.status == 429:  # Rate limited
                        print_highlighted(f"Rate limited. Cooling down for {self.rate_limit_cooldown} seconds...")
                        await asyncio.sleep(self.rate_limit_cooldown)
                        retries += 1
                        continue
                    else:
                        print_highlighted(f"Error {response.status} fetching match {match_id}")
                        return None
            except Exception as e:
                print_highlighted(f"Error fetching match {match_id}: {str(e)}")
                return None
        
        return None

    def format_new_name(self, original_name: str, date: datetime) -> str:
        """Format new filename with date prefix"""
        # Format: MM-DD-YY_original_name
        date_prefix = date.strftime("%m-%d-%y")
        return f"{date_prefix}_{original_name}"

    def write_to_month_file(self, month_dir: str, filename: str, file_type: str):
        """Write match ID to appropriate month file in sorted order"""
        month_name = os.path.basename(month_dir).lower()
        target_file = os.path.join(month_dir, f"{file_type}_matchids_{month_name}.txt")
        
        # Read existing content
        existing_lines = []
        if os.path.exists(target_file):
            with open(target_file, 'r') as f:
                existing_lines = [line.strip() for line in f if line.strip()]
        
        # Add new entry if not exists
        if filename not in existing_lines:
            existing_lines.append(filename)
            
        # Sort lines (newer dates will naturally sort to bottom due to date prefix)
        existing_lines.sort()
        
        # Write back all content
        with open(target_file, 'w') as f:
            for line in existing_lines:
                f.write(f"{line}\n")

    async def process_match(self, session: aiohttp.ClientSession, filename: str, 
                          file_type: str) -> Optional[MatchDate]:
        """Process a single match"""
        try:
            # 1. Extract match ID from filename
            original_name, match_id = self.extract_match_id(filename)
            
            # 2. Check if match is already processed BEFORE making API call
            is_processed, month = self.is_match_already_processed(match_id)
            if is_processed:
                # Print detailed skip message
                skip_message = [
                    f"Skipping match {match_id}",
                    f"Original name: {original_name}",
                    f"Already processed in: {month}",
                    "Found in processed.txt"
                ]
                print_highlighted("\n".join(skip_message))
                
                self.stats['skipped'] += 1
                return None

            # 3. Only make API call if we haven't processed this match yet
            match_date = await self.fetch_match_date(session, match_id)
            if not match_date:
                self.stats['failed'] += 1
                return None

            # Determine month folder and create new name
            month_name = match_date.strftime("%B")  # Full month name
            if month_name not in ['January', 'December']:
                print_highlighted(f"Unexpected month {month_name} for match {match_id}")
                self.stats['failed'] += 1
                return None

            month_dir = os.path.join(self.textfiles_dir, month_name)
            new_name = self.format_new_name(original_name, match_date)

            # Update statistics
            self.stats['processed'] += 1
            self.stats[month_name.lower()] += 1

            # Add to processed.txt
            self._add_to_processed(match_id)

            return MatchDate(
                match_id=match_id,
                original_name=original_name,
                date=match_date,
                month_folder=month_dir,
                new_name=new_name
            )

        except Exception as e:
            print_highlighted(f"Error processing match {filename}: {str(e)}")
            self.stats['failed'] += 1
            return None

    async def process_file(self, file_type: str):
        """Process all matches in a specific file"""
        file_path = os.path.join(self.undated_dir, f"{file_type}_matchids.txt")
        if not os.path.exists(file_path):
            print_highlighted(f"File not found: {file_path}")
            return

        # Read all matches
        with open(file_path, 'r') as f:
            matches = [line.strip() for line in f if line.strip()]

        if not matches:
            print_highlighted(f"No matches found in {file_path}")
            return

        self.stats['total'] += len(matches)

        async with aiohttp.ClientSession() as session:
            for match in matches:
                result = await self.process_match(session, match, file_type)
                if result:
                    # Write to appropriate month file
                    self.write_to_month_file(result.month_folder, result.new_name, file_type)
                    
                    print_highlighted(
                        f"Processed match {result.match_id}\n"
                        f"Date: {result.date.strftime('%Y-%m-%d %H:%M:%S')}\n"
                        f"New name: {result.new_name}"
                    )

    async def process_matches(self):
        """Process all undated matches"""
        try:
            # Ensure month directories exist
            self.ensure_month_directories()
            
            # Process ace matches first, then quad matches
            for file_type in ['ace', 'quad']:
                print_highlighted(f"Processing {file_type} matches...")
                await self.process_file(file_type)

            # Print final statistics
            stats_output = [
                "\nDate fetching complete!",
                f"Total matches processed: {self.stats['total']}",
                f"Successfully processed: {self.stats['processed']}",
                f"Skipped (already processed): {self.stats['skipped']}",
                f"January matches: {self.stats['january']}",
                f"December matches: {self.stats['december']}",
                f"Failed to process: {self.stats['failed']}"
            ]
            print_highlighted("\n".join(stats_output))

            return True

        except Exception as e:
            print_highlighted(f"Error during date fetching: {str(e)}")
            return False

async def start_date_fetching():
    """Entry point for date fetching"""
    try:
        fetcher = DateFetcher()
        return await fetcher.process_matches()
    except Exception as e:
        print_highlighted(f"Error during date fetching: {str(e)}")
        return False

if __name__ == "__main__":
    asyncio.run(start_date_fetching())
