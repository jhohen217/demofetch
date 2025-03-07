import json
import os
import requests
import sys
from pprint import pprint

def load_config():
    """Load configuration from config.json"""
    config_path = "C:/demofetch/config.json"
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading config: {str(e)}")
        sys.exit(1)

def test_hub_api():
    """Test accessing the FACEIT Hub API"""
    # Load configuration
    config = load_config()
    api_key = config['faceit']['api_key']
    
    # Hub ID from the URL
    hub_id = "c7dc4af7-33ad-4973-90c2-5cce9376258b"
    
    # API endpoint
    base_url = "https://open.faceit.com/data/v4"
    endpoint = f"{base_url}/hubs/{hub_id}/matches"
    
    # Headers with API key
    headers = {
        "accept": "application/json",
        "Authorization": f"Bearer {api_key}"
    }
    
    # Parameters
    params = {
        "type": "past",  # Get completed matches
        "offset": 0,
        "limit": 20      # Start with a small number for testing
    }
    
    print(f"Testing FACEIT Hub API access for hub ID: {hub_id}")
    print(f"Using API key: {api_key[:5]}...{api_key[-5:]}")
    print("\nMaking API request...")
    
    try:
        # Make the request
        response = requests.get(endpoint, headers=headers, params=params)
        response.raise_for_status()  # Raise exception for HTTP errors
        
        # Parse the response
        data = response.json()
        
        # Display results
        print(f"\nAPI request successful! Status code: {response.status_code}")
        print(f"Retrieved {len(data.get('items', []))} matches")
        
        # Display match details
        if 'items' in data and data['items']:
            print("\nFirst match details:")
            match = data['items'][0]
            print(f"Match ID: {match.get('match_id')}")
            print(f"Status: {match.get('status')}")
            print(f"Started At: {match.get('started_at')}")
            print(f"Finished At: {match.get('finished_at')}")
            
            # Show all match data for debugging
            print("\nFull match data (first match):")
            pprint(match)
            
            # Save sample response to file for reference
            with open("hub_api_sample_response.json", "w") as f:
                json.dump(data, f, indent=4)
            print("\nSample response saved to hub_api_sample_response.json")
        else:
            print("\nNo matches found in the response")
            print("\nFull API response:")
            pprint(data)
            
    except requests.exceptions.RequestException as e:
        print(f"\nError making API request: {str(e)}")
        if hasattr(e, 'response') and e.response is not None:
            print(f"Status code: {e.response.status_code}")
            print(f"Response text: {e.response.text}")
    except Exception as e:
        print(f"\nUnexpected error: {str(e)}")

if __name__ == "__main__":
    test_hub_api()
