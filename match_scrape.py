import json
import os
import asyncio
import aiohttp
from typing import List, Set, Optional

class MatchScraper:
    def __init__(self):
        # Load configuration
        with open('config.json', 'r') as f:
            self.config = json.load(f)

        self.project_dir = self.config.get('project', {}).get('directory', os.path.dirname(os.path.abspath(__file__)))
        self.textfiles_dir = os.path.join(self.project_dir, 'textfiles')
        os.makedirs(self.textfiles_dir, exist_ok=True)

        # File paths
        self.output_json = os.path.join(self.textfiles_dir, "output.json")
        self.match_ids_file = os.path.join(self.textfiles_dir, "match_ids.txt")

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

    async def fetch_matches(self) -> Optional[dict]:
        """Fetch match data from FACEIT API"""
        retries = 0
        async with aiohttp.ClientSession() as session:
            while retries < self.max_retries:
                try:
                    async with session.get(self.url, headers=self.headers, params=self.params) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:  # Rate limited
                            print(f"Rate limited. Cooling down for {self.rate_limit_cooldown} seconds...")
                            await asyncio.sleep(self.rate_limit_cooldown)
                            retries += 1
                            continue
                        else:
                            print(f"Error fetching data: {response.status}")
                            return None
                except Exception as e:
                    print(f"Network error: {str(e)}")
                    return None
                
                await asyncio.sleep(self.rate_limit_delay)
        
        print("Max retries reached")
        return None

    def extract_match_ids(self, data: dict) -> List[str]:
        """Extract match IDs from API response"""
        match_ids = []
        if "payload" in data and isinstance(data["payload"], list):
            for item in data["payload"]:
                if isinstance(item, dict):
                    match_id = item.get("matchId")
                    if match_id:
                        match_ids.append(match_id)
                else:
                    print("Warning: Payload item is not a dictionary.")
        else:
            print("Error: 'payload' not found or not a list.")
        return match_ids

    async def process_matches(self) -> bool:
        """Main processing function"""
        try:
            print("\nStarting match scraping...")
            
            # Fetch new matches
            data = await self.fetch_matches()
            if not data:
                print("Failed to fetch match data")
                return False

            # Save raw data
            with open(self.output_json, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=4)
            print("Data written to output.json")

            # Extract match IDs
            match_ids = self.extract_match_ids(data)
            if not match_ids:
                print("No match IDs found in payload")
                return False

            # Read existing matches
            existing_matches = []
            if os.path.exists(self.match_ids_file):
                with open(self.match_ids_file, "r", encoding="utf-8") as f:
                    existing_matches = [line.strip() for line in f if line.strip()]

            existing_set = set(existing_matches)

            # Process new matches
            new_matches = []
            consecutive_duplicates = 0

            for mid in match_ids:
                if mid not in existing_set:
                    new_matches.append(mid)
                    consecutive_duplicates = 0
                else:
                    consecutive_duplicates += 1
                    if consecutive_duplicates > 5:
                        print("More than 5 consecutive duplicates found. Stopping.")
                        break

            # Report results
            if new_matches:
                print(f"Found {len(new_matches)} new match{'es' if len(new_matches) != 1 else ''}")
                # Prepend new matches
                updated_matches = new_matches + existing_matches
                # Write updated list
                with open(self.match_ids_file, "w", encoding="utf-8") as f:
                    for mid in updated_matches:
                        f.write(mid + "\n")
                print("Successfully updated match_ids.txt")
            else:
                print("No new matches found")

            return True

        except Exception as e:
            print(f"Error during match processing: {str(e)}")
            return False

async def start_match_scraping():
    """Entry point for match scraping"""
    try:
        scraper = MatchScraper()
        return await scraper.process_matches()
    except Exception as e:
        print(f"Error during match scraping: {str(e)}")
        return False

if __name__ == "__main__":
    asyncio.run(start_match_scraping())