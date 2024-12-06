import os
import json
import time
import asyncio
import aiohttp
import logging
from typing import Optional, Set, List, Dict, Tuple
from dataclasses import dataclass
from concurrent.futures import ThreadPoolExecutor
from collections import defaultdict

@dataclass
class MatchResult:
    match_id: str
    textfiles_dir: str
    has_ace: bool = False
    has_quad: bool = False
    ace_players: List[str] = None
    quad_players: List[str] = None

    def __post_init__(self):
        self.ace_players = self.ace_players or []
        self.quad_players = self.quad_players or []

    @property
    def category(self) -> str:
        if self.has_ace and self.has_quad:
            return "Ace+Quad"
        elif self.has_ace:
            return "Ace"
        elif self.has_quad:
            return "Quad"
        else:
            return "Unapproved"

    @property
    def target_file(self) -> str:
        if self.has_ace and self.has_quad:
            return os.path.join(self.textfiles_dir, "acequad_matchids.txt")
        elif self.has_ace:
            return os.path.join(self.textfiles_dir, "ace_matchids.txt")
        elif self.has_quad:
            return os.path.join(self.textfiles_dir, "quad_matchids.txt")
        else:
            return os.path.join(self.textfiles_dir, "unapproved_matchids.txt")

class MatchProcessor:
    def __init__(self):
        self.config = self.load_config()
        self.project_dir = os.path.abspath(self.config["project"]["directory"])
        self.api_key = self.config["faceit"]["api_key"]
        self.textfiles_dir = os.path.join(self.project_dir, "textfiles")
        
        # Create textfiles directory if it doesn't exist
        os.makedirs(self.textfiles_dir, exist_ok=True)
        print(f"\nInitializing Match Processor")
        print(f"Project directory: {self.project_dir}")
        print(f"Textfiles directory: {self.textfiles_dir}\n")

        # Initialize file paths
        self.match_ids_file = os.path.join(self.textfiles_dir, "match_ids.txt")
        self.filter_queue_file = os.path.join(self.textfiles_dir, "filter_queue.txt")
        self.filtered_file = os.path.join(self.textfiles_dir, "match_filtered.txt")
        self.unapproved_file = os.path.join(self.textfiles_dir, "unapproved_matchids.txt")
        self.ace_file = os.path.join(self.textfiles_dir, "ace_matchids.txt")
        self.quad_file = os.path.join(self.textfiles_dir, "quad_matchids.txt")
        self.acequad_file = os.path.join(self.textfiles_dir, "acequad_matchids.txt")

        # API configuration
        self.api_base_url = "https://open.faceit.com/data/v4/matches/{match_id}/stats"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Accept": "application/json"
        }

        # Rate limiting parameters
        self.rate_limit_delay = 0.2  # 200ms between requests
        self.max_retries = 3
        self.rate_limit_cooldown = 60  # 1 minute cooldown if rate limited
        self.max_concurrent_requests = 5  # Maximum number of concurrent API requests

        # Statistics
        self.stats = defaultdict(int)

    @staticmethod
    def load_config():
        """Load configuration from config.json"""
        script_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(script_dir, "config.json")
        
        try:
            with open(config_path, "r") as f:
                return json.load(f)
        except Exception as e:
            print(f"Error loading config from {config_path}: {str(e)}")
            raise

    def ensure_file_exists(self, filepath: str):
        """Ensure a file exists, create it if it doesn't"""
        try:
            if not os.path.exists(filepath):
                # Ensure directory exists
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                with open(filepath, "w") as f:
                    f.write("")  # Create empty file
        except Exception as e:
            print(f"Error creating file {filepath}: {str(e)}")
            raise

    def write_to_file_with_flush(self, filepath: str, match_id: str):
        """Helper function to safely write a match_id to a file"""
        try:
            self.ensure_file_exists(filepath)
            with open(filepath, "a") as f:
                f.write(match_id + "\n")
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
        except Exception as e:
            print(f"Error writing to {filepath}: {str(e)}")
            raise

    def initialize_filter_queue(self):
        """
        Ensures filter_queue.txt exists and is populated with unfiltered matches.
        """
        print("Initializing filter queue...")
        try:
            self.ensure_file_exists(self.filter_queue_file)
            self.ensure_file_exists(self.match_ids_file)
            
            # Read all match IDs
            with open(self.match_ids_file, "r") as f:
                all_matches = {line.strip() for line in f if line.strip()}

            # Read filtered matches
            filtered_matches = set()
            if os.path.exists(self.filtered_file):
                with open(self.filtered_file, "r") as f:
                    filtered_matches = {line.strip() for line in f if line.strip()}

            # Find unfiltered matches
            unfiltered_matches = all_matches - filtered_matches

            # Write unfiltered matches to queue
            with open(self.filter_queue_file, "w") as f:
                for match_id in unfiltered_matches:
                    f.write(f"{match_id}\n")
                    f.flush()

            print(f"Added {len(unfiltered_matches)} unfiltered matches to queue")

        except Exception as e:
            print(f"Error initializing filter queue: {str(e)}")
            raise

    async def fetch_scoreboard(self, session: aiohttp.ClientSession, match_id: str, 
                             semaphore: asyncio.Semaphore) -> Optional[dict]:
        """
        Fetch the scoreboard JSON for a given match_id.
        Returns a dictionary if successful, None otherwise.
        Includes rate limiting protection.
        """
        url = self.api_base_url.format(match_id=match_id)
        retries = 0
        
        async with semaphore:  # Limit concurrent requests
            while retries < self.max_retries:
                try:
                    async with session.get(url, headers=self.headers) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:  # Rate limited
                            print(f"Rate limited. Cooling down for {self.rate_limit_cooldown} seconds...")
                            await asyncio.sleep(self.rate_limit_cooldown)
                            retries += 1
                            continue
                        else:
                            print(f"Failed to fetch match {match_id}: HTTP {response.status}")
                            return None
                except Exception as e:
                    print(f"Network error for match {match_id}: {str(e)}")
                    return None
                
                await asyncio.sleep(self.rate_limit_delay)  # Add delay between requests
        
        print(f"Max retries reached for match {match_id}")
        return None

    def analyze_match(self, match_data: dict) -> MatchResult:
        """
        Analyze the match data and return a MatchResult object.
        Looks at the "player_stats" field for "Penta Kills" and "Quadro Kills".
        """
        result = MatchResult(match_id=match_data.get("match_id", "unknown"), textfiles_dir=self.textfiles_dir)

        rounds = match_data.get("rounds", [])
        for rnd in rounds:
            teams = rnd.get("teams", [])
            for team in teams:
                players = team.get("players", [])
                for player in players:
                    player_stats = player.get("player_stats", {})
                    nickname = player.get("nickname", "unknown")

                    # Convert stats to integers, defaulting to 0 if missing
                    penta = int(player_stats.get("Penta Kills", "0"))
                    quadro = int(player_stats.get("Quadro Kills", "0"))

                    if penta > 0:
                        result.has_ace = True
                        result.ace_players.append(nickname)
                    if quadro > 0:
                        result.has_quad = True
                        result.quad_players.append(nickname)

        return result

    async def process_match(self, session: aiohttp.ClientSession, match_id: str, 
                          filtered_matches: Set[str], semaphore: asyncio.Semaphore) -> Optional[str]:
        """Process a single match"""
        if not match_id or match_id in filtered_matches:
            return None

        try:
            # Fetch and analyze match
            match_data = await self.fetch_scoreboard(session, match_id, semaphore)
            if match_data is None:
                self.stats['failed'] += 1
                return match_id  # Return match_id to retry later

            # Analyze match data
            result = self.analyze_match(match_data)
            result.match_id = match_id

            # Update statistics
            if result.has_ace and result.has_quad:
                self.stats['acequad'] += 1
            elif result.has_ace:
                self.stats['ace'] += 1
            elif result.has_quad:
                self.stats['quad'] += 1
            else:
                self.stats['unapproved'] += 1

            # Print match results in a concise format
            print(f"\nMatch {match_id}")
            if result.quad_players:
                print(f"Found quad by {', '.join(result.quad_players)}")
            if result.ace_players:
                print(f"Found ace by {', '.join(result.ace_players)}")
            print(f"Successfully wrote match to {os.path.basename(result.target_file)}")

            # Write result to appropriate files
            self.write_to_file_with_flush(result.target_file, match_id)
            self.write_to_file_with_flush(self.filtered_file, match_id)
            
            # Remove from filter queue
            return None

        except Exception as e:
            print(f"Error processing match {match_id}: {str(e)}")
            self.stats['failed'] += 1
            return match_id  # Return match_id to retry later

    async def process_matches(self):
        """Process matches with rate limiting protection"""
        try:
            # Ensure all required files exist
            for filepath in [self.match_ids_file, self.filter_queue_file, self.filtered_file,
                           self.unapproved_file, self.ace_file, self.quad_file, self.acequad_file]:
                self.ensure_file_exists(filepath)
            
            # Initialize filter queue with unfiltered matches
            self.initialize_filter_queue()

            # Read filtered matches into a set for quick lookup
            filtered_matches = set()
            with open(self.filtered_file, "r") as f:
                filtered_matches = {line.strip() for line in f if line.strip()}
                print(f"\nFound {len(filtered_matches)} previously filtered matches")

            # Read matches from filter queue
            with open(self.filter_queue_file, "r") as fq:
                queue = [line.strip() for line in fq if line.strip()]
                unfiltered_matches = [m for m in queue if m not in filtered_matches]
                print(f"Found {len(unfiltered_matches)} unfiltered matches to process\n")

            if not unfiltered_matches:
                print("No new matches to process")
                return True

            self.stats['total'] = len(unfiltered_matches)

            # Create semaphore to limit concurrent requests
            semaphore = asyncio.Semaphore(self.max_concurrent_requests)
            
            # Process matches concurrently
            async with aiohttp.ClientSession() as session:
                tasks = [self.process_match(session, match_id, filtered_matches, semaphore) 
                        for match_id in unfiltered_matches]
                results = await asyncio.gather(*tasks)

            # Collect matches that need to be retried
            new_filter_queue = [mid for mid in results if mid is not None]

            # Update filter queue with remaining matches
            try:
                # Write updated queue
                with open(self.filter_queue_file, "w") as fq:
                    for mid in new_filter_queue:
                        fq.write(mid + "\n")
                        fq.flush()
                print(f"\nUpdated filter queue with {len(new_filter_queue)} remaining matches")
            except Exception as e:
                print(f"Error updating filter queue: {str(e)}")
                raise

            print("\nFiltering complete!")
            print(f"Total matches processed: {self.stats['total']}")
            print(f"Ace matches: {self.stats['ace']}")
            print(f"Quad matches: {self.stats['quad']}")
            print(f"Ace+Quad matches: {self.stats['acequad']}")
            print(f"Unapproved matches: {self.stats['unapproved']}")
            print(f"Failed to process: {self.stats['failed']}")
            print(f"Remaining in queue: {len(new_filter_queue)}")

            return True

        except Exception as e:
            print(f"Error during match filtering: {str(e)}")
            return False

async def start_match_filtering():
    """Entry point for match filtering"""
    try:
        processor = MatchProcessor()
        return await processor.process_matches()
    except Exception as e:
        print(f"Error during match filtering: {str(e)}")
        return False

if __name__ == "__main__":
    asyncio.run(start_match_filtering())
