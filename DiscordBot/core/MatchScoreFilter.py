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
        
        # New files for enhanced error tracking and retry management
        self.failed_matches_log = os.path.join(self.month_dir, f"failed_matches_log_{month_lower}.json")
        self.permanent_fail_file = os.path.join(self.month_dir, f"permanent_fails_{month_lower}.txt")

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
        
        # Retry management
        self.max_retry_attempts = 5  # Maximum number of retry attempts for a match
        
        # Initialize retry counts from failed_matches_log if it exists
        self.retry_counts = {}
        if os.path.exists(self.failed_matches_log):
            try:
                with open(self.failed_matches_log, 'r') as f:
                    failed_log = json.load(f)
                    for match_id, data in failed_log.items():
                        self.retry_counts[match_id] = data.get('retry_count', 0)
            except Exception as e:
                print_highlighted(f"Error loading retry counts: {e}")

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

    def log_failed_match(self, match_id: str, error_type: str, error_details: str, retry_count: int = 0):
        """Log detailed information about a failed match."""
        try:
            # Create or load existing log
            failed_log = {}
            if os.path.exists(self.failed_matches_log):
                with open(self.failed_matches_log, 'r') as f:
                    failed_log = json.load(f)
            
            # Add or update entry
            timestamp = datetime.now().isoformat()
            failed_log[match_id] = {
                'error_type': error_type,
                'error_details': error_details,
                'last_retry': timestamp,
                'retry_count': retry_count,
                'status': 'pending'  # pending, permanent_fail, resolved
            }
            
            # Write updated log
            with open(self.failed_matches_log, 'w') as f:
                json.dump(failed_log, f, indent=2)
                
        except Exception as e:
            print_highlighted(f"Error logging failed match: {e}")

    async def fetch_match_data(self, session: aiohttp.ClientSession, match_id: str,
                             semaphore: asyncio.Semaphore) -> Tuple[Optional[dict], Optional[Tuple[str, str]]]:
        """
        Fetch both match details and stats for a given match_id.
        Returns a tuple of (match_data, error_info), where error_info is (error_type, error_details) if fetch fails.
        """
        url = self.api_base_url.format(match_id=match_id)
        retries = 0

        async with semaphore:  # Limit concurrent requests
            while retries < self.max_retries:
                try:
                    async with session.get(url, headers=self.headers) as response:
                        if response.status == 200:
                            return await response.json(), None
                        elif response.status == 429:
                            error_type = "rate_limited"
                            error_details = f"Rate limited - Status: {response.status}"
                            rate_limit_msg = f"[FILTER] {error_details} - Trying again in {self.rate_limit_cooldown} seconds..."
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
                            error_type = "api_error"
                            error_details = f"API error - Status: {response.status}"
                            try:
                                error_json = await response.json()
                                if error_json:
                                    error_details += f", Details: {json.dumps(error_json)}"
                            except:
                                error_body = await response.text()
                                if error_body:
                                    error_details += f", Body: {error_body[:200]}"
                            
                            return None, (error_type, error_details)
                except Exception as e:
                    error_type = "network_error"
                    error_details = f"Network error: {str(e)}"
                    print_highlighted(f"An error occurred: {e}")
                    return None, (error_type, error_details)

                await asyncio.sleep(self.rate_limit_delay)

        return None, ("max_retries_exceeded", f"Exceeded maximum retries ({self.max_retries})")

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

    async def process_match(self, session: aiohttp.ClientSession, match_id: str, timestamp: str,
                          filtered_matches: Set[str], semaphore: asyncio.Semaphore) -> Optional[str]:
        """Process a single match, now with timestamp and enhanced error tracking."""
        if not match_id or match_id in filtered_matches:
            return None

        # Get current retry count
        retry_count = self.retry_counts.get(match_id, 0)
        self.retry_counts[match_id] = retry_count + 1
        
        # Check if we've exceeded retry limit
        if retry_count >= self.max_retry_attempts:
            print_highlighted(f"Match {match_id} has exceeded maximum retry attempts ({self.max_retry_attempts}). Marking as permanent fail.")
            self.log_failed_match(match_id, "max_retries_exceeded", 
                                f"Exceeded maximum retry attempts ({self.max_retry_attempts})", 
                                retry_count)
            return match_id  # Will be moved to permanent fails

        try:
            # Fetch and analyze match
            match_stats, error_info = await self.fetch_match_data(session, match_id, semaphore)

            if match_stats is None:
                self.stats['failed'] += 1
                
                # Log detailed error information
                if error_info:
                    error_type, error_details = error_info
                    self.log_failed_match(match_id, error_type, error_details, retry_count + 1)
                    
                    # Print more detailed error message
                    print_highlighted(f"Match {match_id} failed: {error_type} - {error_details}")
                else:
                    self.log_failed_match(match_id, "unknown_error", "No error details available", retry_count + 1)
                
                return match_id  # Return match_id to retry later

            # print_highlighted(f"Match stats: {match_stats}")

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
            
            # Update status in failed_matches_log if it was previously failing
            if match_id in self.retry_counts and os.path.exists(self.failed_matches_log):
                try:
                    with open(self.failed_matches_log, 'r') as f:
                        failed_log = json.load(f)
                    
                    if match_id in failed_log:
                        failed_log[match_id]['status'] = 'resolved'
                        failed_log[match_id]['resolved_at'] = datetime.now().isoformat()
                        
                        with open(self.failed_matches_log, 'w') as f:
                            json.dump(failed_log, f, indent=2)
                except Exception as e:
                    print_highlighted(f"Error updating failed log: {e}")

            return None  # Match processed successfully

        except Exception as e:
            error_msg = f"Error processing match {match_id}: {str(e)}"
            print_highlighted(error_msg)
            self.stats['failed'] += 1
            self.log_failed_match(match_id, "processing_error", error_msg, retry_count + 1)
            return match_id  # Return match_id to retry later

    async def cleanup_filter_queue(self):
        """Clean up the filter queue by removing duplicates and checking for stuck matches."""
        try:
            # Ensure filter queue file exists
            self.ensure_file_exists(self.filter_queue_file)
            
            # Read current filter queue
            current_queue = []
            if os.path.exists(self.filter_queue_file):
                with open(self.filter_queue_file, 'r') as f:
                    current_queue = [line.strip() for line in f if line.strip()]
            
            if not current_queue:
                print_highlighted("Filter queue is empty, nothing to clean up.")
                return True
            
            # Check for duplicates
            unique_queue = []
            seen = set()
            duplicates = 0
            
            for match_id in current_queue:
                if match_id not in seen:
                    seen.add(match_id)
                    unique_queue.append(match_id)
                else:
                    duplicates += 1
            
            # Update filter queue with unique matches
            with open(self.filter_queue_file, 'w') as f:
                for match_id in unique_queue:
                    f.write(f"{match_id}\n")
                    f.flush()
            
            print_highlighted(f"Filter queue cleanup complete. Removed {duplicates} duplicate entries.")
            return True
            
        except Exception as e:
            print_highlighted(f"Error during filter queue cleanup: {str(e)}")
            return False

    async def process_matches(self):
        """Process matches with rate limiting protection and retry limits"""
        try:
            # Ensure all required files exist
            for filepath in [self.match_ids_file, self.filter_queue_file, self.filtered_file,
                           self.unapproved_file, self.ace_file, self.quad_file, 
                           self.failed_matches_log, self.permanent_fail_file]:
                self.ensure_file_exists(filepath)
            
            # Clean up filter queue before processing
            await self.cleanup_filter_queue()
            
            # Initialize filter queue with unfiltered matches
            self.initialize_filter_queue()

            # Read all matches and filtered matches
            all_matches = []
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

            # Collect matches that need to be retried and those that have exceeded retry limits
            new_filter_queue = []
            permanent_fails = []
            
            for mid in results:
                if mid is not None:
                    # Check if we've exceeded retry limit
                    if self.retry_counts.get(mid, 0) >= self.max_retry_attempts:
                        permanent_fails.append(mid)
                        
                        # Update status in failed_matches_log
                        if os.path.exists(self.failed_matches_log):
                            try:
                                with open(self.failed_matches_log, 'r') as f:
                                    failed_log = json.load(f)
                                
                                if mid in failed_log:
                                    failed_log[mid]['status'] = 'permanent_fail'
                                    
                                with open(self.failed_matches_log, 'w') as f:
                                    json.dump(failed_log, f, indent=2)
                            except Exception as e:
                                print_highlighted(f"Error updating failed log: {e}")
                    else:
                        new_filter_queue.append(mid)
            
            # Write permanent fails to file
            if permanent_fails:
                try:
                    # Read existing permanent fails
                    existing_fails = set()
                    if os.path.exists(self.permanent_fail_file):
                        with open(self.permanent_fail_file, 'r') as f:
                            existing_fails = {line.strip() for line in f if line.strip()}
                    
                    # Add new permanent fails
                    with open(self.permanent_fail_file, 'a') as f:
                        for mid in permanent_fails:
                            if mid not in existing_fails:
                                f.write(f"{mid}\n")
                                f.flush()
                except Exception as e:
                    print_highlighted(f"Error writing permanent fails: {e}")

            # Update filter queue with remaining matches
            try:
                with open(self.filter_queue_file, "w") as fq:
                    for mid in new_filter_queue:
                        fq.write(mid + "\n")
                        fq.flush()
            except Exception as e:
                print_highlighted(f"Error updating filter queue: {str(e)}")
                raise

            # Print final statistics with enhanced information
            stats_output = [
                "\nFiltering complete!",
                f"Total matches processed: {self.stats['total']}",
                f"Ace matches: {self.stats['ace']}",
                f"Quad matches: {self.stats['quad']}",
                f"Unapproved matches: {self.stats['unapproved']}",
                f"Failed to process: {self.stats['failed']}",
                f"Permanent fails: {len(permanent_fails)}",
                f"Remaining in queue: {len(new_filter_queue)}"
            ]
            print_highlighted("\n".join(stats_output))

            return True

        except Exception as e:
            print_highlighted(f"Error during match filtering: {str(e)}")
            return False

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
