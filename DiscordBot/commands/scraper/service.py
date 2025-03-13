"""
Core service functionality for the scraper module.
"""

import os
import json
import asyncio
import aiohttp
import logging
from typing import Set, List, Dict, Any, Optional, Tuple
from datetime import datetime

from commands.scraper.config import get_config, get_month_files
from commands.scraper.utils import ensure_file_exists, print_highlighted

# Set up logging
logger = logging.getLogger('discord_bot')

class MatchScraper:
    """
    Class for scraping matches from the FACEIT API.
    """
    def __init__(self, bot=None, month=None):
        """
        Initialize the match scraper.
        
        Args:
            bot: Discord bot instance
            month: Month to scrape (e.g., "February")
        """
        # Discord bot instance
        self.bot = bot
        
        # Load configuration
        self.config = get_config()
        
        # Use configured directories
        self.project_dir = self.config['project']['directory']
        self.textfiles_dir = self.config['project']['textfiles_directory']
        
        # Get current month if not specified
        self.month = month or datetime.now().strftime("%B")  # e.g., "February"
        self.month_dir = os.path.join(self.textfiles_dir, self.month)
        self.month_lower = self.month.lower()
        os.makedirs(self.month_dir, exist_ok=True)

        # Get file paths
        files = get_month_files(self.month)
        self.output_json = files['output_json']
        self.match_ids_file = files['match_ids_file']
        self.permanent_fail_file = files['permanent_fail_file']
        
        # Print header
        print("\n" + "="*50)
        print("           FACEIT Demo Manager")
        print("="*50 + "\n")
        
        # Load permanent fails
        self.permanent_fails = self.load_permanent_fails()
        
        # API configuration
        self.url = "https://www.faceit.com/api/match-history/v4/matches/competition"
        self.headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json"
        }
        self.params = {
            "page": 4,
            "size": 20,
            "id": "3aced33b-f21c-450c-91d5-10535164e0ab",
            "type": "matchmaking"
        }

        # Rate limiting
        self.rate_limit_delay = 0.2  # 200ms between requests
        self.max_retries = 3
        self.rate_limit_cooldown = 60  # 1 minute cooldown if rate limited
        
    def load_permanent_fails(self) -> Set[str]:
        """
        Load the list of permanently failed match IDs.
        
        Returns:
            Set[str]: Set of permanently failed match IDs
        """
        permanent_fails = set()
        if os.path.exists(self.permanent_fail_file):
            try:
                with open(self.permanent_fail_file, "r", encoding="utf-8") as f:
                    permanent_fails = {line.strip() for line in f if line.strip()}
                print(f"Loaded {len(permanent_fails)} permanently failed matches")
            except Exception as e:
                print(f"Error loading permanent fails: {str(e)}")
        return permanent_fails
        
    def cleanup_match_ids(self) -> bool:
        """
        Remove permanently failed matches from match_ids file.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            if not os.path.exists(self.match_ids_file) or not self.permanent_fails:
                return True
                
            # Read existing matches
            existing_matches = []
            with open(self.match_ids_file, "r", encoding="utf-8") as f:
                for line in f:
                    if line.strip():
                        parts = line.strip().split(',')
                        match_id = parts[0]
                        timestamp = parts[1] if len(parts) > 1 else ""
                        existing_matches.append((match_id, timestamp))
            
            # Filter out permanent fails
            filtered_matches = [(mid, ts) for mid, ts in existing_matches if mid not in self.permanent_fails]
            removed_count = len(existing_matches) - len(filtered_matches)
            
            if removed_count > 0:
                # Write filtered matches back to file
                with open(self.match_ids_file, "w", encoding="utf-8") as f:
                    for match_id, timestamp in filtered_matches:
                        f.write(f"{match_id},{timestamp}\n")
                print(f"Removed {removed_count} permanently failed matches from match_ids file")
            
            return True
        except Exception as e:
            print(f"Error cleaning up match IDs: {str(e)}")
            return False

    async def fetch_matches(self) -> Optional[Dict[str, Any]]:
        """
        Fetch match data from FACEIT API.
        
        Returns:
            Optional[Dict[str, Any]]: Match data or None if failed
        """
        retries = 0
        async with aiohttp.ClientSession() as session:
            while retries < self.max_retries:
                try:
                    print("\nFetching matches from FACEIT API...")
                    async with session.get(self.url, headers=self.headers, params=self.params) as response:
                        if response.status == 200:
                            print("Successfully received response from API")
                            return await response.json()
                        elif response.status == 429:  # Rate limited
                            rate_limit_msg = f"[SCRAPER] Rate limited - Trying again in {self.rate_limit_cooldown} seconds..."
                            print(rate_limit_msg)
                            
                            # Send DM if bot instance is available
                            if self.bot and self.bot.owner:
                                try:
                                    await self.bot.send_message(self.bot.owner, rate_limit_msg)
                                except:
                                    pass  # Ignore DM sending failures
                                    
                            await asyncio.sleep(self.rate_limit_cooldown)
                            retries += 1
                            continue
                        else:
                            print(f"Error fetching data: HTTP {response.status}")
                            return None
                except Exception as e:
                    print(f"Network error: {str(e)}")
                    return None
                
                await asyncio.sleep(self.rate_limit_delay)
        
        print("Max retries reached")
        return None

    def extract_match_data(self, data: Dict[str, Any]) -> List[Tuple[str, str, str]]:
        """
        Extract match ID, finishedAt timestamp, and status from API response.
        
        Args:
            data: API response data
            
        Returns:
            List[Tuple[str, str, str]]: List of (match_id, finished_at, status) tuples
        """
        match_data = []
        if "payload" in data and isinstance(data["payload"], list):
            for item in data["payload"]:
                if isinstance(item, dict):
                    match_id = item.get("matchId")
                    finished_at = item.get("finishedAt")
                    status = item.get("status", "")  # Get status if available
                    if match_id and finished_at:
                        match_data.append((match_id, finished_at, status))
                else:
                    print("Warning: Payload item is not a dictionary.")
        else:
            print("Error: 'payload' not found or not a list.")

        print(f"Found {len(match_data)} matches in response")
        return match_data

    async def process_matches(self) -> bool:
        """
        Main processing function.
        
        Returns:
            bool: True if successful, False otherwise
        """
        try:
            print("\n" + "="*50)
            print("Starting match scraping process...")
            print("="*50 + "\n")
            
            # First, clean up any permanent fails from the match_ids file
            print("Cleaning up permanent fails from match_ids file...")
            self.cleanup_match_ids()

            # Fetch new matches
            data = await self.fetch_matches()
            if not data:
                print("Failed to fetch match data")
                return False

            # Save raw data
            with open(self.output_json, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            print("Raw data written to output.json")

            # Extract match IDs and timestamps
            match_data = self.extract_match_data(data)  # Now returns (match_id, finished_at, status) tuples
            if not match_data:
                print("No match IDs found in payload")
                return False

            # Read existing matches
            existing_matches = set()
            if os.path.exists(self.match_ids_file):
                with open(self.match_ids_file, "r", encoding="utf-8") as f:
                    # Read only the match ID part
                    existing_matches = {line.strip().split(',')[0] for line in f if line.strip()}
                print(f"\nFound {len(existing_matches)} existing matches in match_ids.txt")

            # Process new matches
            new_matches = []
            consecutive_duplicates = 0
            skipped_permanent_fails = 0
            skipped_cancelled_matches = 0

            print("\nChecking for new matches...")
            for match_id, finished_at, status in match_data:
                # Skip if match is in permanent fails list
                if match_id in self.permanent_fails:
                    skipped_permanent_fails += 1
                    continue
                
                # Skip if match is cancelled
                if status and status.upper() == "CANCELLED":
                    skipped_cancelled_matches += 1
                    print(f"Skipping cancelled match: {match_id}")
                    continue
                    
                if match_id not in existing_matches:
                    new_matches.append((match_id, finished_at))
                    consecutive_duplicates = 0
                else:
                    consecutive_duplicates += 1
                    if consecutive_duplicates > 5:
                        print("\nMore than 5 consecutive duplicates found. Stopping.")
                        break
            
            if skipped_permanent_fails > 0:
                print(f"\nSkipped {skipped_permanent_fails} matches that were in permanent fails list")
                
            if skipped_cancelled_matches > 0:
                print(f"\nSkipped {skipped_cancelled_matches} cancelled matches")

            # Report results
            if new_matches:
                print(f"\nFound {len(new_matches)} new matches")
                # Read all existing matches to preserve them
                existing_match_data = []
                if os.path.exists(self.match_ids_file):
                    with open(self.match_ids_file, "r", encoding="utf-8") as f:
                        for line in f:
                            if line.strip():
                                parts = line.strip().split(',')
                                match_id = parts[0]
                                timestamp = parts[1] if len(parts) > 1 else ""
                                existing_match_data.append((match_id, timestamp))
                
                # Prepend new matches (match_id, timestamp)
                updated_matches = new_matches + existing_match_data
                
                # Write updated list: match_id,finished_at
                with open(self.match_ids_file, "w", encoding="utf-8") as f:
                    for match_id, finished_at in updated_matches:
                        f.write(f"{match_id},{finished_at}\n")
                
                print(f"Successfully updated match_ids.txt (Total: {len(updated_matches)} matches)")
            else:
                print("\nNo new matches found")

            print("\n" + "="*50)
            print("Match scraping process complete")
            print("="*50 + "\n")
            return True

        except Exception as e:
            print(f"Error during match processing: {str(e)}")
            return False

async def start_match_scraping(bot=None, month=None):
    """
    Entry point for match scraping.
    
    Args:
        bot: Discord bot instance
        month: Month to scrape (e.g., "February")
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        scraper = MatchScraper(bot, month)
        return await scraper.process_matches()
    except Exception as e:
        print(f"Error during match scraping: {str(e)}")
        return False
