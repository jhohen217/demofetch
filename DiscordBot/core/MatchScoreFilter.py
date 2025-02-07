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
from datetime import datetime

def print_highlighted(message: str):
    """Print a message in a highlighted format"""
    print(f"\n{message}")

@dataclass
class MatchResult:
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
        # Format: MM-DD-YY_XXXX_matchid where:
        # - MM-DD-YY is the match date
        # - XXXX is ace count and quad count
        # - matchid is the original match ID
        date_prefix = self.match_date.strftime("%m-%d-%y") if self.match_date else "00-00-00"
        count_prefix = f"{self.ace_count:02d}{self.quad_count:02d}"
        return f"{date_prefix}_{count_prefix}_{self.match_id}"

    @property
    def target_file(self) -> str:
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

class MatchProcessor:
    def __init__(self, bot=None):
        # Discord bot instance
        self.bot = bot
        # Load configuration from project root
        core_dir = os.path.dirname(os.path.abspath(__file__))  # core directory
        project_dir = os.path.dirname(core_dir)  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(project_dir), 'config.json')
        
        with open(config_path, 'r') as f:
            self.config = json.load(f)

        # Use configured directories
        self.project_dir = self.config['project']['directory']
        self.api_key = self.config["faceit"]["api_key"]
        self.textfiles_dir = self.config['project']['textfiles_directory']
        
        # Get current month directory and name
        current_month = datetime.now().strftime("%B")  # e.g., "February"
        self.month_dir = os.path.join(self.textfiles_dir, current_month)
        month_lower = current_month.lower()
        
        # Create month directory if it doesn't exist
        os.makedirs(self.month_dir, exist_ok=True)

        # Initialize file paths with month directory and month suffix
        self.match_ids_file = os.path.join(self.month_dir, f"match_ids_{month_lower}.txt")
        self.filter_queue_file = os.path.join(self.month_dir, f"filter_queue_{month_lower}.txt")
        self.filtered_file = os.path.join(self.month_dir, f"match_filtered_{month_lower}.txt")
        self.unapproved_file = os.path.join(self.month_dir, f"unapproved_matchids_{month_lower}.txt")
        self.ace_file = os.path.join(self.month_dir, f"ace_matchids_{month_lower}.txt")
        self.quad_file = os.path.join(self.month_dir, f"quad_matchids_{month_lower}.txt")

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

    def ensure_file_exists(self, filepath: str):
        """Ensure a file exists, create it if it doesn't"""
        try:
            if not os.path.exists(filepath):
                # Ensure directory exists
                os.makedirs(os.path.dirname(filepath), exist_ok=True)
                with open(filepath, "w") as f:
                    f.write("")  # Create empty file
        except Exception as e:
            print_highlighted(f"Error creating file {filepath}: {str(e)}")
            raise

    def write_to_file_with_flush(self, filepath: str, match_id: str, formatted: bool = False):
        """Helper function to safely write a match_id to a file"""
        try:
            self.ensure_file_exists(filepath)
            
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
                    print_highlighted(f"Match {match_id} already exists in {filepath}, skipping...")
                    return
            else:
                if match_id in existing_lines:
                    print_highlighted(f"Match {match_id} already exists in {filepath}, skipping...")
                    return
            
            # Add new match_id
            existing_lines.append(match_id)
            
            # Sort if this is the ace_file or quad_file (formatted IDs)
            if formatted and (filepath == self.ace_file or filepath == self.quad_file):
                existing_lines.sort()  # Simple alphabetical sort
            
            # Write back all content
            with open(filepath, "w") as f:
                for line in existing_lines:
                    f.write(line + "\n")
                f.flush()
                os.fsync(f.fileno())  # Force write to disk
        except Exception as e:
            print_highlighted(f"Error writing to {filepath}: {str(e)}")
            raise

    def initialize_filter_queue(self):
        """
        Ensures filter_queue.txt exists and is populated with unfiltered matches.
        """
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

        except Exception as e:
            print_highlighted(f"Error initializing filter queue: {str(e)}")
            raise

    async def fetch_match_data(self, session: aiohttp.ClientSession, match_id: str,
                             semaphore: asyncio.Semaphore) -> Tuple[Optional[dict], Optional[dict]]:
        """
        Fetch both match details and stats for a given match_id.
        Returns a tuple of (match_details, match_stats), either can be None if fetch fails.
        """
        url = self.api_base_url.format(match_id=match_id)
        retries = 0

        async with semaphore:  # Limit concurrent requests
            while retries < self.max_retries:
                try:
                    async with session.get(url, headers=self.headers) as response:
                        if response.status == 200:
                            return await response.json(), None  # Return None for the second value
                        elif response.status == 429:
                            rate_limit_msg = f"[FILTER] Rate limited - Trying again in {self.rate_limit_cooldown} seconds..."
                            print_highlighted(rate_limit_msg)

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
                            return None, None
                except Exception as e:
                    print_highlighted(f"An error occurred: {e}")
                    return None, None  # Ensure we return a tuple

                await asyncio.sleep(self.rate_limit_delay)

        return None, None

    def analyze_match(self, match_data: dict) -> MatchResult:
        """Analyze the match data and return a MatchResult object."""
        result = MatchResult(match_id="unknown", textfiles_dir=self.textfiles_dir)
        try:
            result.match_id = match_data.get("match_id", "unknown") if match_data else "unknown"

            rounds = match_data.get("rounds", []) if match_data else []
            for rnd in rounds:
                teams = rnd.get("teams", []) if rnd else []
                for team in teams:
                    players = team.get("players", []) if team else []
                    for player in players:
                        player_stats = player.get("player_stats", {}) if player else {}
                        nickname = player.get("nickname", "unknown") if player else "unknown"

                        # Convert stats to integers, defaulting to 0 if missing
                        penta = int(player_stats.get("Penta Kills", "0")) if player_stats else 0
                        quadro = int(player_stats.get("Quadro Kills", "0")) if player_stats else 0

                        result.ace_count += penta
                        result.quad_count += quadro

                        if penta > 0:
                            result.has_ace = True
                            result.ace_players.append(nickname)
                        if quadro > 0:
                            result.has_quad = True
                            result.quad_players.append(nickname)
        except Exception as e:
            print_highlighted(f"Error analyzing match data: {e}, Data: {match_data}")
            return MatchResult(match_id="error", textfiles_dir=self.textfiles_dir)

        return result

    async def process_match(self, session: aiohttp.ClientSession, match_id: str, 
                          filtered_matches: Set[str], semaphore: asyncio.Semaphore) -> Optional[str]:
        """Process a single match"""
        if not match_id or match_id in filtered_matches:
            return None

        try:
            # Fetch and analyze match
            match_stats, _ = await self.fetch_match_data(session, match_id, semaphore)
            if match_stats is None:
                self.stats['failed'] += 1
                return match_id  # Return match_id to retry later

            # Extract timestamp from match details, if available (it might not be)
            timestamp = match_stats.get('started_at', match_stats.get('finished_at'))
            if timestamp:
                try:
                    # Faceit API returns timestamps in ISO 8601 format (UTC)
                    result.match_date = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).replace(tzinfo=None)
                except Exception as e:
                    print_highlighted(f"Error converting timestamp: {e}, Timestamp: {timestamp}")

            # Analyze match stats
            result = self.analyze_match(match_stats)
            result.match_id = match_id

            # Update statistics
            if result.has_ace:
                self.stats['ace'] += 1
            if result.has_quad:
                self.stats['quad'] += 1
            if not (result.has_ace or result.has_quad):
                self.stats['unapproved'] += 1

            # Print match results in a highlighted format
            output_lines = []
            output_lines.append(f"Match {match_id}")
            if result.quad_players:
                output_lines.append(f"Found {result.quad_count} quad kills by {', '.join(result.quad_players)}")
            if result.ace_players:
                output_lines.append(f"Found {result.ace_count} ace kills by {', '.join(result.ace_players)}")
            output_lines.append(f"Successfully wrote match to {os.path.basename(result.target_file)}")
            
            print_highlighted("\n".join(output_lines))

            # Write result to appropriate files
            if result.has_ace:  # Save to ace_matchids.txt if it has any ace kills
                self.write_to_file_with_flush(result.target_file, result.formatted_match_id, formatted=True)
            elif result.has_quad:  # Save to quad_matchids.txt if it has any quad kills
                self.write_to_file_with_flush(result.target_file, result.formatted_match_id, formatted=True)
            else:
                self.write_to_file_with_flush(result.target_file, match_id)
            
            self.write_to_file_with_flush(self.filtered_file, match_id)
            
            # Remove from filter queue
            return None

        except Exception as e:
            print_highlighted(f"Error processing match {match_id}: {str(e)}")
            self.stats['failed'] += 1
            return match_id  # Return match_id to retry later

    async def process_matches(self):
        """Process matches with rate limiting protection"""
        try:
            # Ensure all required files exist
            for filepath in [self.match_ids_file, self.filter_queue_file, self.filtered_file,
                           self.unapproved_file, self.ace_file, self.quad_file]:  # Added quad_file
                self.ensure_file_exists(filepath)
            
            # Initialize filter queue with unfiltered matches
            self.initialize_filter_queue()

            # Read all matches and filtered matches
            all_matches = set()
            filtered_matches = set()
            
            with open(self.match_ids_file, "r") as f:
                all_matches = [line.strip().split(',') for line in f if line.strip()]  # Now a list of [match_id, timestamp]

            if os.path.exists(self.filtered_file):
                with open(self.filtered_file, "r") as f:
                    filtered_matches = {line.strip() for line in f if line.strip()}

            # Find unfiltered matches (compare only match IDs)
            unfiltered_matches = [m for m in all_matches if m[0] not in filtered_matches]

            if not unfiltered_matches:
                return True

            self.stats['total'] = len(unfiltered_matches)

            # Create semaphore to limit concurrent requests
            semaphore = asyncio.Semaphore(self.max_concurrent_requests)

            # Process matches concurrently, passing match_id *and* timestamp
            async with aiohttp.ClientSession() as session:
                tasks = [self.process_match(session, match_id, timestamp, filtered_matches, semaphore)
                         for match_id, timestamp in unfiltered_matches]
                results = await asyncio.gather(*tasks)

            # Collect matches that need to be retried (only match_id is returned)
            new_filter_queue = [mid for mid in results if mid is not None]

            # Update filter queue with remaining matches (only match_id, no timestamp)
            try:
                with open(self.filter_queue_file, "w") as fq:
                    for mid in new_filter_queue:
                        fq.write(mid + "\n")
                        fq.flush()
            except Exception as e:
                print_highlighted(f"Error updating filter queue: {str(e)}")
                raise

            # Print final statistics in highlighted format
            stats_output = [
                "\nFiltering complete!",
                f"Total matches processed: {self.stats['total']}",
                f"Ace matches: {self.stats['ace']}",
                f"Quad matches: {self.stats['quad']}",
                f"Unapproved matches: {self.stats['unapproved']}",
                f"Failed to process: {self.stats['failed']}",
                f"Remaining in queue: {len(new_filter_queue)}"
            ]
            print_highlighted("\n".join(stats_output))

            return True

        except Exception as e:
            print_highlighted(f"Error during match filtering: {str(e)}")
            return False

    async def process_match(self, session: aiohttp.ClientSession, match_id: str, 
                          filtered_matches: Set[str], semaphore: asyncio.Semaphore) -> Optional[str]:
        """Process a single match"""
        if not match_id or match_id in filtered_matches:
            return None

        try:
            # Fetch and analyze match
            match_stats, _ = await self.fetch_match_data(session, match_id, semaphore)
            if match_stats is None:
                self.stats['failed'] += 1
                return match_id  # Return match_id to retry later

            # Extract timestamp from match details, if available (it might not be)
            timestamp = match_stats.get('started_at', match_stats.get('finished_at'))
            if timestamp:
                try:
                    # Faceit API returns timestamps in ISO 8601 format (UTC)
                    result.match_date = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).replace(tzinfo=None)
                except Exception as e:
                    print_highlighted(f"Error converting timestamp: {e}, Timestamp: {timestamp}")

            # Analyze match stats
            result = self.analyze_match(match_stats)
            result.match_id = match_id

            # Update statistics
            if result.has_ace:
                self.stats['ace'] += 1
            if result.has_quad:
                self.stats['quad'] += 1
            if not (result.has_ace or result.has_quad):
                self.stats['unapproved'] += 1

            # Print match results in a highlighted format
            output_lines = []
            output_lines.append(f"Match {match_id}")
            if result.quad_players:
                output_lines.append(f"Found {result.quad_count} quad kills by {', '.join(result.quad_players)}")
            if result.ace_players:
                output_lines.append(f"Found {result.ace_count} ace kills by {', '.join(result.ace_players)}")
            output_lines.append(f"Successfully wrote match to {os.path.basename(result.target_file)}")
            
            print_highlighted("\n".join(output_lines))

            # Write result to appropriate files
            if result.has_ace:  # Save to ace_matchids.txt if it has any ace kills
                self.write_to_file_with_flush(result.target_file, result.formatted_match_id, formatted=True)
            elif result.has_quad:  # Save to quad_matchids.txt if it has any quad kills
                self.write_to_file_with_flush(result.target_file, result.formatted_match_id, formatted=True)
            else:
                self.write_to_file_with_flush(result.target_file, match_id)
            
            self.write_to_file_with_flush(self.filtered_file, match_id)
            
            # Remove from filter queue
            return None

        except Exception as e:
            print_highlighted(f"Error processing match {match_id}: {str(e)}")
            self.stats['failed'] += 1
            return match_id  # Return match_id to retry later

    async def process_match(self, session: aiohttp.ClientSession, match_id: str, timestamp: str,
                          filtered_matches: Set[str], semaphore: asyncio.Semaphore) -> Optional[str]:
        """Process a single match, now with timestamp."""
        if not match_id or match_id in filtered_matches:
            return None

        try:
            # Fetch and analyze match
            match_stats, _ = await self.fetch_match_data(session, match_id, semaphore)

            if match_stats is None:
                self.stats['failed'] += 1
                return match_id  # Return match_id to retry later

            print_highlighted(f"Match stats: {match_stats}")

            result = self.analyze_match(match_stats)
            result.match_id = match_id  # Set the match ID

            # Use provided timestamp
            if timestamp:
                try:
                    result.match_date = datetime.fromisoformat(timestamp.replace("Z", "+00:00")).replace(tzinfo=None)
                except Exception as e:
                    print_highlighted(f"Error converting timestamp: {e}, Timestamp: {timestamp}")
            else:
                print_highlighted(f"Warning: No timestamp for match {match_id}")

            # Update statistics
            if result.has_ace:
                self.stats['ace'] += 1
            if result.has_quad:
                self.stats['quad'] += 1
            if not (result.has_ace or result.has_quad):
                self.stats['unapproved'] += 1

            # Print match results
            output_lines = [
                f"Match {match_id}",
                f"  Date: {result.match_date.strftime('%Y-%m-%d %H:%M:%S') if result.match_date else 'No date'}",
                f"  Ace Players: {', '.join(result.ace_players) if result.ace_players else 'None'}",
                f"  Quad Players: {', '.join(result.quad_players) if result.quad_players else 'None'}",
                f"  Target File: {os.path.basename(result.target_file)}",
            ]
            print_highlighted("\n".join(output_lines))


            # Write result to appropriate files
            if result.has_ace:
                self.write_to_file_with_flush(result.target_file, result.formatted_match_id, formatted=True)
            elif result.has_quad:
                self.write_to_file_with_flush(result.target_file, result.formatted_match_id, formatted=True)
            else:
                self.write_to_file_with_flush(result.target_file, match_id)

            self.write_to_file_with_flush(self.filtered_file, match_id)

            return None  # Match processed (or skipped)

        except Exception as e:
            print_highlighted(f"Error processing match {match_id}: {str(e)}")
            self.stats['failed'] += 1
            return match_id  # Return match_id to retry later

async def start_match_filtering(bot=None):
    """Entry point for match filtering"""
    try:
        processor = MatchProcessor(bot)
        return await processor.process_matches()
    except Exception as e:
        print_highlighted(f"Error during match filtering: {str(e)}")
        return False

if __name__ == "__main__":
    asyncio.run(start_match_filtering())
