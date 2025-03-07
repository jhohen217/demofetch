import json
import os
import asyncio
import aiohttp
from typing import List, Set, Optional, Tuple
from datetime import datetime

class HubScraper:
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
        self.textfiles_dir = self.config['project']['textfiles_directory']
        
        # Get current month directory and name
        current_month = datetime.now().strftime("%B")  # e.g., "February"
        self.month_dir = os.path.join(self.textfiles_dir, current_month)
        month_lower = current_month.lower()
        os.makedirs(self.month_dir, exist_ok=True)

        print("\n" + "="*50)
        print("           FACEIT Hub Demo Manager")
        print("="*50 + "\n")

        # File paths with month suffix - use the same files as the regular match scraper
        self.output_json = os.path.join(self.month_dir, f"hub_output_{month_lower}.json")
        self.match_ids_file = os.path.join(self.month_dir, f"match_ids_{month_lower}.txt")

        # Hub ID from the URL
        self.hub_id = "c7dc4af7-33ad-4973-90c2-5cce9376258b"
        
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

    async def fetch_hub_matches(self) -> Optional[dict]:
        """Fetch match data from FACEIT Hub API"""
        retries = 0
        async with aiohttp.ClientSession() as session:
            while retries < self.max_retries:
                try:
                    print("\nFetching matches from FACEIT Hub API...")
                    async with session.get(self.url, headers=self.headers, params=self.params) as response:
                        if response.status == 200:
                            print("Successfully received response from Hub API")
                            return await response.json()
                        elif response.status == 429:  # Rate limited
                            rate_limit_msg = f"[HUB SCRAPER] Rate limited - Trying again in {self.rate_limit_cooldown} seconds..."
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
                            print(f"Error fetching hub data: HTTP {response.status}")
                            return None
                except Exception as e:
                    print(f"Network error in hub scraper: {str(e)}")
                    return None
                
                await asyncio.sleep(self.rate_limit_delay)
        
        print("Max retries reached in hub scraper")
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
                    print("Warning: Hub item is not a dictionary.")
        else:
            print("Error: 'items' not found or not a list in hub response.")
        
        print(f"Found {len(match_ids)} matches in hub response")
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
                        match_data.append((match_id, str(finished_at)))
                else:
                    print("Warning: Hub item is not a dictionary.")
        else:
            print("Error: 'items' not found or not a list in hub response.")

        print(f"Found {len(match_data)} matches in hub response")
        return match_data

    async def process_hub_matches(self) -> bool:
        """Main processing function for hub matches"""
        try:
            print("\n" + "="*50)
            print("Starting hub match scraping process...")
            print("="*50 + "\n")

            # Fetch new matches
            data = await self.fetch_hub_matches()
            if not data:
                print("Failed to fetch hub match data")
                return False

            # Save raw data
            with open(self.output_json, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            print("Raw hub data written to hub_output.json")

            # Extract match IDs and timestamps
            match_data = self.extract_match_data(data)  # Returns (match_id, finished_at) tuples
            if not match_data:
                print("No match IDs found in hub payload")
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

            print("\nChecking for new hub matches...")
            for match_id, finished_at in match_data:
                if match_id not in existing_matches:
                    new_matches.append((match_id, finished_at))
                    consecutive_duplicates = 0
                else:
                    consecutive_duplicates += 1
                    if consecutive_duplicates > 5:
                        print("\nMore than 5 consecutive duplicates found in hub matches. Stopping.")
                        break

            # Report results
            if new_matches:
                print(f"\nFound {len(new_matches)} new hub matches")
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
                print(f"Successfully updated match_ids.txt with hub matches (Total: {len(updated_matches)} matches)")
            else:
                print("\nNo new hub matches found")

            print("\n" + "="*50)
            print("Hub match scraping process complete")
            print("="*50 + "\n")
            return True

        except Exception as e:
            print(f"Error during hub match processing: {str(e)}")
            return False

async def start_hub_scraping(bot=None):
    """Entry point for hub match scraping"""
    try:
        scraper = HubScraper(bot)
        return await scraper.process_hub_matches()
    except Exception as e:
        print(f"Error during hub match scraping: {str(e)}")
        return False

if __name__ == "__main__":
    asyncio.run(start_hub_scraping())
