#!/usr/bin/env python3
"""
Script to test the response from Faceit hubs configured in config.json.
This script tests the connection to the Faceit API and retrieves match data from the hubs.
"""

import os
import sys
import json
import urllib.request
import urllib.parse
import urllib.error
from datetime import datetime

# Add the parent directory to sys.path to import from core
current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.dirname(current_dir)
sys.path.append(parent_dir)

# Create a simplified version of the HubScraper class to avoid dependencies
class SimpleHubScraper:
    def __init__(self, hub_id, hub_name):
        # Load configuration from the Windows path
        config_path = os.path.join('..', '..', 'Users', 'josh', 'discordbot_pi', 'config.json')
        
        with open(config_path, 'r') as f:
            self.config = json.load(f)
        
        # Hub ID and name
        self.hub_id = hub_id
        self.hub_name = hub_name
        
        print(f"\nTesting hub: {self.hub_name} (ID: {self.hub_id})")
        
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

    def fetch_hub_matches(self):
        """Fetch match data from FACEIT Hub API"""
        try:
            print("\nFetching matches from FACEIT Hub API...")
            
            # Add query parameters to URL
            url = self.url
            if self.params:
                query_string = urllib.parse.urlencode(self.params)
                url = f"{url}?{query_string}"
            
            # Create request with headers
            req = urllib.request.Request(url, headers=self.headers)
            
            # Send request and get response
            with urllib.request.urlopen(req) as response:
                if response.getcode() == 200:
                    print("Successfully received response from Hub API")
                    data = response.read().decode('utf-8')
                    return json.loads(data)
                else:
                    print(f"Error fetching hub data: HTTP {response.getcode()}")
                    return None
        except urllib.error.HTTPError as e:
            print(f"HTTP Error: {e.code} - {e.reason}")
            return None
        except urllib.error.URLError as e:
            print(f"URL Error: {e.reason}")
            return None
        except Exception as e:
            print(f"Network error in hub scraper: {str(e)}")
            return None

def test_single_hub(hub_id, hub_name):
    """Test a single hub by ID and name."""
    print(f"\n{'='*60}")
    print(f"Testing hub: {hub_name} (ID: {hub_id})")
    print(f"{'='*60}")
    
    # Create a SimpleHubScraper instance for this hub
    scraper = SimpleHubScraper(hub_id, hub_name)
    
    # Fetch hub matches
    data = scraper.fetch_hub_matches()
    
    if not data:
        print("\nFailed to fetch data from hub API.")
        return False
    
    # Print API response summary
    print("\nAPI Response Summary:")
    print(f"  Status: Success")
    
    # Extract and print match data
    if "items" in data and isinstance(data["items"], list):
        match_count = len(data["items"])
        print(f"  Matches found: {match_count}")
        
        if match_count > 0:
            print("\nSample match data (first match):")
            sample_match = data["items"][0]
            print(f"  Match ID: {sample_match.get('match_id', 'N/A')}")
            print(f"  Status: {sample_match.get('status', 'N/A')}")
            print(f"  Started At: {sample_match.get('started_at', 'N/A')}")
            print(f"  Finished At: {sample_match.get('finished_at', 'N/A')}")
            
            # Print teams if available
            if "teams" in sample_match:
                print("\n  Teams:")
                for team_name, team_data in sample_match["teams"].items():
                    print(f"    {team_name}: {team_data.get('nickname', 'N/A')}")
                    if "players" in team_data:
                        print("    Players:")
                        for player in team_data["players"]:
                            print(f"      {player.get('nickname', 'N/A')}")
    else:
        print("  No matches found in response.")
    
    print(f"\n{'='*60}")
    print(f"Test completed for hub: {hub_name}")
    print(f"{'='*60}")
    
    return True

def main():
    """Main function to run the tests."""
    print("\nFaceit Hub API Test")
    print("===================\n")
    
    # Load configuration from the Windows path
    config_path = os.path.join('..', '..', 'Users', 'josh', 'discordbot_pi', 'config.json')
    
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    # Get hub list from config
    hubs = config.get('faceit', {}).get('hubs', [])
    
    if not hubs:
        print("No hubs defined in config.json.")
        return
    
    print(f"Found {len(hubs)} hubs in config.json:")
    for i, hub in enumerate(hubs):
        print(f"  {i+1}. {hub.get('name', 'Unnamed')} (ID: {hub.get('id', 'No ID')})")
    
    # Test each hub individually
    print("\nTesting each hub individually:")
    
    # Test Cache NA Challenge hub
    hub_id_na = "c7dc4af7-33ad-4973-90c2-5cce9376258b"
    hub_name_na = "Cache NA Challenge"
    test_single_hub(hub_id_na, hub_name_na)
    
    # Test Cache EU Challenge hub
    hub_id_eu = "55f14f39-24a0-4be6-a37f-0e558e5e2950"
    hub_name_eu = "Cache EU Challenge"
    test_single_hub(hub_id_eu, hub_name_eu)

if __name__ == "__main__":
    main()
