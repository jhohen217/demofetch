import json
import os
import asyncio
import aiohttp
import logging
from typing import List, Set, Optional, Tuple
from datetime import datetime

# Set up logging
logger = logging.getLogger('discord_bot')

class HubScraper:
    def __init__(self, bot=None, hub_id=None, hub_name=None):
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
        self.textfiles_dir = self.config['project']['textfiles_directory']
        
        # Get current month directory and name
        current_month = datetime.now().strftime("%B")  # e.g., "February"
        self.month_dir = os.path.join(self.textfiles_dir, current_month)
        month_lower = current_month.lower()
        os.makedirs(self.month_dir, exist_ok=True)

        # File paths with month suffix - use the same files as the regular match scraper
        self.output_json = os.path.join(self.month_dir, f"hub_output_{month_lower}.json")
        self.match_ids_file = os.path.join(self.month_dir, f"match_ids_{month_lower}.txt")
        self.permanent_fail_file = os.path.join(self.month_dir, f"permanent_fails_{month_lower}.txt")

        # Hub ID and name (either provided or default)
        self.hub_id = hub_id or "c7dc4af7-33ad-4973-90c2-5cce9376258b"
        self.hub_name = hub_name or "Default Hub"
        
        # Load permanent fails
        self.permanent_fails = self.load_permanent_fails()
        
        logger.info("\n" + "="*50)
        logger.info(f"           FACEIT Hub Demo Manager - {self.hub_name}")
        logger.info("="*50 + "\n")
        
        # API configuration
        self.base_url = "https://open.faceit.com/data/v4"
        self.url = f"{self.base_url}/hubs/{self.hub_id}/matches"
        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.config['faceit']['api_key']}"
        }
        self.params = {
            "offset": 0,
            "limit": 20,
            "type": "past"  # Get completed matches
        }

        # Rate limiting
        self.rate_limit_delay = 0.2  # 200ms between requests
        self.max_retries = 3
        self.rate_limit_cooldown = 60  # 1 minute cooldown if rate limited
        
    def load_permanent_fails(self) -> Set[str]:
        """Load the list of permanently failed match IDs"""
        permanent_fails = set()
        if os.path.exists(self.permanent_fail_file):
            try:
                with open(self.permanent_fail_file, "r", encoding="utf-8") as f:
                    permanent_fails = {line.strip() for line in f if line.strip()}
                logger.info(f"Loaded {len(permanent_fails)} permanently failed matches")
            except Exception as e:
                logger.error(f"Error loading permanent fails: {str(e)}")
        return permanent_fails
        
    def cleanup_match_ids(self) -> bool:
        """Remove permanently failed matches from match_ids file"""
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
                logger.info(f"Removed {removed_count} permanently failed matches from match_ids file")
            
            return True
        except Exception as e:
            logger.error(f"Error cleaning up match IDs: {str(e)}")
            return False

    async def fetch_hub_matches(self) -> Optional[dict]:
        """Fetch match data from FACEIT Hub API"""
        retries = 0
        async with aiohttp.ClientSession() as session:
            while retries < self.max_retries:
                try:
                    logger.info("\nFetching matches from FACEIT Hub API...")
                    async with session.get(self.url, headers=self.headers, params=self.params) as response:
                        if response.status == 200:
                            logger.info("Successfully received response from Hub API")
                            return await response.json()
                        elif response.status == 429:  # Rate limited
                            rate_limit_msg = f"[HUB SCRAPER] Rate limited - Trying again in {self.rate_limit_cooldown} seconds..."
                            logger.warning(rate_limit_msg)
                            
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
                            logger.error(f"Error fetching hub data: HTTP {response.status}")
                            return None
                except Exception as e:
                    logger.error(f"Network error in hub scraper: {str(e)}")
                    return None
                
                await asyncio.sleep(self.rate_limit_delay)
        
        logger.error("Max retries reached in hub scraper")
        return None

    def extract_match_ids(self, data: dict) -> List[str]:
        """Extract match IDs from API response"""
        match_ids = []
        if "items" in data and isinstance(data["items"], list):
            for item in data["items"]:
                if isinstance(item, dict):
                    match_id = item.get("match_id")
                    if match_id:
                        match_ids.append(match_id)
                else:
                    logger.warning("Hub item is not a dictionary.")
        else:
            logger.error("'items' not found or not a list in hub response.")
        
        logger.info(f"Found {len(match_ids)} matches in hub response")
        return match_ids

    def extract_match_data(self, data: dict) -> List[Tuple[str, str]]:
        """Extract match ID and finishedAt timestamp from API response."""
        match_data = []
        if "items" in data and isinstance(data["items"], list):
            for item in data["items"]:
                if isinstance(item, dict):
                    match_id = item.get("match_id")
                    finished_at = item.get("finished_at")
                    if match_id and finished_at:
                        # Convert Unix timestamp to ISO format if it's a number
                        try:
                            if isinstance(finished_at, (int, str)) and str(finished_at).isdigit():
                                # Convert Unix timestamp (seconds since epoch) to ISO format
                                from datetime import datetime
                                dt = datetime.fromtimestamp(int(finished_at))
                                iso_timestamp = dt.isoformat()
                                match_data.append((match_id, iso_timestamp))
                            else:
                                # Already in ISO format or other format
                                match_data.append((match_id, str(finished_at)))
                        except Exception as e:
                            logger.error(f"Error converting timestamp for match {match_id}: {e}")
                            # Still add the match with original timestamp
                            match_data.append((match_id, str(finished_at)))
                else:
                    logger.warning("Hub item is not a dictionary.")
        else:
            logger.error("'items' not found or not a list in hub response.")

        logger.info(f"Found {len(match_data)} matches in hub response")
        return match_data

    async def process_hub_matches(self) -> bool:
        """Main processing function for hub matches"""
        try:
            logger.info("\n" + "="*50)
            logger.info("Starting hub match scraping process...")
            logger.info("="*50 + "\n")
            
            # First, clean up any permanent fails from the match_ids file
            logger.info("Cleaning up permanent fails from match_ids file...")
            self.cleanup_match_ids()

            # Fetch new matches
            data = await self.fetch_hub_matches()
            if not data:
                logger.error("Failed to fetch hub match data")
                return False

            # Save raw data
            with open(self.output_json, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            logger.info("Raw hub data written to hub_output.json")

            # Extract match IDs and timestamps
            match_data = self.extract_match_data(data)  # Returns (match_id, finished_at) tuples
            if not match_data:
                logger.warning("No match IDs found in hub payload")
                return False

            # Read existing matches
            existing_matches = set()
            if os.path.exists(self.match_ids_file):
                with open(self.match_ids_file, "r", encoding="utf-8") as f:
                    # Read only the match ID part
                    existing_matches = {line.strip().split(',')[0] for line in f if line.strip()}
                logger.info(f"\nFound {len(existing_matches)} existing matches in match_ids.txt")

            # Process new matches
            new_matches = []
            consecutive_duplicates = 0
            skipped_permanent_fails = 0
            skipped_cancelled_matches = 0

            logger.info("\nChecking for new hub matches...")
            for item in data.get("items", []):
                if not isinstance(item, dict):
                    continue
                    
                match_id = item.get("match_id")
                finished_at = item.get("finished_at")
                status = item.get("status")
                
                if not match_id or not finished_at:
                    continue
                
                # Convert Unix timestamp to ISO format if it's a number
                try:
                    if isinstance(finished_at, (int, str)) and str(finished_at).isdigit():
                        # Convert Unix timestamp (seconds since epoch) to ISO format
                        dt = datetime.fromtimestamp(int(finished_at))
                        iso_timestamp = dt.isoformat()
                    else:
                        # Already in ISO format or other format
                        iso_timestamp = str(finished_at)
                except Exception as e:
                    logger.error(f"Error converting timestamp for match {match_id}: {e}")
                    iso_timestamp = str(finished_at)
                
                # Skip if match is in permanent fails list
                if match_id in self.permanent_fails:
                    skipped_permanent_fails += 1
                    continue
                
                # Skip if match is cancelled
                if status == "CANCELLED":
                    skipped_cancelled_matches += 1
                    logger.info(f"Skipping cancelled match: {match_id}")
                    continue
                    
                if match_id not in existing_matches:
                    new_matches.append((match_id, iso_timestamp))
                    consecutive_duplicates = 0
                else:
                    consecutive_duplicates += 1
                    if consecutive_duplicates > 5:
                        logger.info("\nMore than 5 consecutive duplicates found in hub matches. Stopping.")
                        break
            
            if skipped_permanent_fails > 0:
                logger.info(f"\nSkipped {skipped_permanent_fails} matches that were in permanent fails list")
                
            if skipped_cancelled_matches > 0:
                logger.info(f"\nSkipped {skipped_cancelled_matches} cancelled matches")

            # Report results
            if new_matches:
                logger.info(f"\nFound {len(new_matches)} new hub matches")
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
                
                # Count matches by category for summary
                ace_count = 0
                quad_count = 0
                unapproved_count = 0
                
                # Get current month name
                current_month = datetime.now().strftime("%B")  # e.g., "February"
                month_dir = os.path.join(self.textfiles_dir, current_month)
                month_lower = current_month.lower()
                
                # Check if category files exist and count matches
                ace_file = os.path.join(month_dir, f"ace_matchids_{month_lower}.txt")
                quad_file = os.path.join(month_dir, f"quad_matchids_{month_lower}.txt")
                unapproved_file = os.path.join(month_dir, f"unapproved_matchids_{month_lower}.txt")
                
                if os.path.exists(ace_file):
                    with open(ace_file, "r", encoding="utf-8") as f:
                        ace_count = sum(1 for line in f if line.strip())
                
                if os.path.exists(quad_file):
                    with open(quad_file, "r", encoding="utf-8") as f:
                        quad_count = sum(1 for line in f if line.strip())
                
                if os.path.exists(unapproved_file):
                    with open(unapproved_file, "r", encoding="utf-8") as f:
                        unapproved_count = sum(1 for line in f if line.strip())
                
                # Print summary similar to regular match scraper
                logger.info(f"\nSuccessfully updated match_ids.txt with hub matches (Total: {len(updated_matches)} matches)")
                logger.info("\nHub Match Categories:")
                logger.info(f"Ace matches: {ace_count}")
                logger.info(f"Quad matches: {quad_count}")
                logger.info(f"Unapproved matches: {unapproved_count}")
            else:
                logger.info("\nNo new hub matches found")

            logger.info("\n" + "="*50)
            logger.info("Hub match scraping process complete")
            logger.info("="*50 + "\n")
            return True

        except Exception as e:
            logger.error(f"Error during hub match processing: {str(e)}")
            return False

async def process_all_hubs(bot=None):
    """Process all hubs defined in the config file"""
    try:
        # Load configuration
        core_dir = os.path.dirname(os.path.abspath(__file__))  # core directory
        project_dir = os.path.dirname(core_dir)  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(project_dir), 'config.json')
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Get hub list from config
        hubs = config.get('faceit', {}).get('hubs', [])
        
        # If no hubs defined, use default
        if not hubs:
            logger.info("No hubs defined in config, using default hub")
            return await start_hub_scraping(bot)
        
        # Process each hub with a delay between them
        overall_result = True
        for i, hub in enumerate(hubs):
            hub_id = hub.get('id')
            hub_name = hub.get('name', f"Hub {i+1}")
            
            if not hub_id:
                logger.warning(f"Skipping hub {hub_name} - no ID provided")
                continue
                
            logger.info(f"\nProcessing hub: {hub_name} ({hub_id})")
            result = await start_hub_scraping(bot, hub_id, hub_name)
            overall_result = overall_result and result
            
            # Wait between hubs (except after the last one)
            if i < len(hubs) - 1:
                logger.info(f"Waiting 60 seconds before processing next hub...")
                await asyncio.sleep(60)
        
        return overall_result
    except Exception as e:
        logger.error(f"Error processing hubs: {str(e)}")
        return False

async def start_hub_scraping(bot=None, hub_id=None, hub_name=None):
    """Entry point for hub match scraping"""
    try:
        scraper = HubScraper(bot, hub_id, hub_name)
        return await scraper.process_hub_matches()
    except Exception as e:
        logger.error(f"Error during hub match scraping: {str(e)}")
        return False

if __name__ == "__main__":
    asyncio.run(start_hub_scraping())
