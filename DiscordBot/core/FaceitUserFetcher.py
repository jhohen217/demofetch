import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import time
import json
import os
import urllib3

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class FaceitUserFetcher:
    def __init__(self):
        # Load configuration from project root
        core_dir = os.path.dirname(os.path.abspath(__file__))  # core directory
        project_dir = os.path.dirname(core_dir)  # DiscordBot directory
        config_path = os.path.join(os.path.dirname(project_dir), 'config.json')
        self.config = self.load_config(config_path)
        self.api_key = self.config['faceit']['api_key']
        self.base_url = "https://open.faceit.com/data/v4"
        self.headers = {
            "accept": "application/json",
            "Authorization": f"Bearer {self.api_key}"
        }
        # Configure session with retries and timeouts
        self.session = self._create_session()

    def _create_session(self):
        """Create a requests session with retry strategy"""
        session = requests.Session()
        retry_strategy = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        session.verify = False
        session.timeout = (5, 30)  # (connect timeout, read timeout)
        return session

    def load_config(self, config_path):
        """Load configuration from config.json"""
        if not os.path.exists(config_path):
            raise FileNotFoundError("config.json not found. Please copy config.json.example to config.json and fill in your API keys.")
        
        with open(config_path, 'r') as f:
            return json.load(f)

    def get_player_id(self, nickname):
        """Get player ID from nickname using the /players endpoint"""
        try:
            endpoint = f"{self.base_url}/players"
            params = {
                "nickname": nickname,
                "game": "cs2"
            }
            
            message = f"Looking up player ID for {nickname}..."
            print(message)
            return message, self._make_request(endpoint, params)
            
        except Exception as e:
            error_msg = f"Error getting player ID: {str(e)}"
            print(error_msg)
            return error_msg, None

    def _make_request(self, endpoint, params):
        """Make API request with error handling"""
        try:
            response = self.session.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            
            data = response.json()
            return data.get("player_id")
        except requests.exceptions.RequestException as e:
            error_msg = f"Error: {str(e)}"
            if hasattr(e, 'response') and e.response is not None:
                error_msg = f"Error: {e.response.status_code} - {e.response.text}"
            print(error_msg)
            return None

    def get_player_history(self, player_id, game="cs2", offset=0, limit=100):
        """Get match history using the /players/{player_id}/history endpoint"""
        try:
            endpoint = f"{self.base_url}/players/{player_id}/history"
            params = {
                "game": game,
                "offset": offset,
                "limit": limit
            }
            
            message = f"Fetching match history (batch {offset//limit + 1})..."
            print(message)
            
            response = self.session.get(endpoint, headers=self.headers, params=params)
            response.raise_for_status()
            
            return message, response.json()
            
        except requests.exceptions.RequestException as e:
            error_msg = f"Error fetching match history: {str(e)}"
            if hasattr(e, 'response') and e.response is not None:
                error_msg = f"Error: {e.response.status_code} - {e.response.text}"
            print(error_msg)
            return error_msg, None

    def get_all_match_ids(self, nickname):
        """Get all match IDs for a player"""
        message, player_id = self.get_player_id(nickname)
        if not player_id:
            error_msg = f"Could not find player ID for {nickname}"
            print(error_msg)
            return message, error_msg, []
            
        status_msg = f"Found player ID: {player_id}"
        print(status_msg)
        
        all_matches = []
        offset = 0
        limit = 100
        messages = [message, status_msg]
        
        while True:
            msg, matches_data = self.get_player_history(player_id, offset=offset, limit=limit)
            messages.append(msg)
            
            if not matches_data:
                break
            
            matches = matches_data.get("items", [])
            if not matches:
                break
            
            for match in matches:
                match_id = match.get("match_id")
                if match_id:
                    all_matches.append(match_id)
            
            batch_msg = f"Found {len(matches)} matches in batch {offset//limit + 1}"
            print(batch_msg)
            messages.append(batch_msg)
            
            if len(matches) < limit:
                break
                
            offset += limit
            time.sleep(1)  # Rate limiting
        
        return "\n".join(messages), None, all_matches

    def save_match_ids(self, matches, username):
        """Save match IDs to a text file with newest matches first"""
        if not matches:
            message = "No matches to save"
            print(message)
            return message, None

        # Get usermatches directory from config
        usermatches_dir = os.path.join(self.config['project']['directory'], "usermatches")
        os.makedirs(usermatches_dir, exist_ok=True)
        
        # Save to usermatches directory
        filename = os.path.join(usermatches_dir, f"{username}.txt")
        try:
            with open(filename, "w") as f:
                for match_id in matches:
                    f.write(f"{match_id}\n")
                    
            message = f"Successfully saved {len(matches)} matches to {filename}"
            print(message)
            return message, filename
        except Exception as e:
            error_msg = f"Error saving matches: {str(e)}"
            print(error_msg)
            return error_msg, None

def fetch_user_matches(username):
    """Fetch and save matches for a given username"""
    try:
        fetcher = FaceitUserFetcher()
        progress_msg, error_msg, matches = fetcher.get_all_match_ids(username)
        
        if error_msg:
            return False, error_msg
        
        if matches:
            save_msg, filename = fetcher.save_match_ids(matches, username)
            if filename:
                return True, f"{progress_msg}\n{save_msg}"
            return False, save_msg
        else:
            return False, f"{progress_msg}\nNo matches found"
    except Exception as e:
        error_msg = f"Error fetching matches: {str(e)}"
        print(error_msg)
        return False, error_msg

if __name__ == "__main__":
    username = input("Enter FACEIT username: ")
    success, message = fetch_user_matches(username)
    print(message)
