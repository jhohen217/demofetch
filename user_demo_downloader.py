import os
import json
import requests
import zipfile
import io
import glob
import urllib3
import time

# Disable SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

class DemoDownloader:
    def __init__(self, config_path='config.json'):
        self.config = self.load_config(config_path)
        self.api_key = self.config['faceit']['api_key']
        self.base_url = "https://api.faceit.com"
        self.headers = {
            "Authorization": f"Bearer {self.api_key}"
        }
        self.quota_info = {}

    def load_config(self, config_path):
        """Load configuration from config.json"""
        if not os.path.exists(config_path):
            raise FileNotFoundError("config.json not found")
        with open(config_path, 'r') as f:
            return json.load(f)

    def update_quota_info(self, response):
        """Update quota information from response headers"""
        self.quota_info = {
            'bytes_total': response.headers.get('x-faceit-downloadquota-bytes-total', '0'),
            'bytes_used': response.headers.get('x-faceit-downloadquota-bytes-used', '0'),
            'total': response.headers.get('x-faceit-downloadquota-total', '0'),
            'used': response.headers.get('x-faceit-downloadquota-used', '0')
        }

    def get_quota_info(self):
        """Get current quota information"""
        return self.quota_info

    def check_existing_demo(self, demos_dir, match_id):
        """Check if demo already exists in unzipped form"""
        demo_path = os.path.join(demos_dir, match_id)
        if os.path.exists(demo_path):
            dem_files = glob.glob(os.path.join(demo_path, "**", "*.dem"), recursive=True)
            if dem_files:
                return True, demo_path
        return False, demo_path

    def download_demo(self, match_id, username):
        """Download and extract a demo for a specific match"""
        demos_dir = f"{username}_demos"
        os.makedirs(demos_dir, exist_ok=True)

        # Check if demo already exists
        exists, demo_path = self.check_existing_demo(demos_dir, match_id)
        if exists:
            return False, f"Demo {match_id} already exists (found .dem file)"

        try:
            # Step 1: Request the download
            url = f'{self.base_url}/match/v2/match/{match_id}/stats'
            print(f"Requesting download for match {match_id}...")
            response = requests.post(url, headers=self.headers)
            self.update_quota_info(response)

            if response.status_code != 200:
                return False, f"Failed to request demo download: {response.status_code}"

            # Step 2: Poll for the download link
            print(f"Polling for download link for match {match_id}...")
            download_url = None
            for attempt in range(10):  # Try polling 10 times
                print(f"Polling attempt {attempt + 1}/10...")
                time.sleep(5)  # Wait for 5 seconds before polling
                poll_response = requests.get(url, headers=self.headers)
                if poll_response.status_code == 200:
                    data = poll_response.json()
                    download_url = data.get('payload', {}).get('url')
                    if download_url:
                        print(f"Download URL received for match {match_id}!")
                        break

            if not download_url:
                return False, "Download URL not available after polling"

            # Step 3: Download and extract the demo
            print(f"Downloading demo for match {match_id}...")
            demo_response = requests.get(download_url)
            if demo_response.status_code != 200:
                return False, f"Failed to download demo: {demo_response.status_code}"

            # Extract the demo
            try:
                print(f"Extracting demo for match {match_id}...")
                with zipfile.ZipFile(io.BytesIO(demo_response.content)) as zip_ref:
                    zip_ref.extractall(demo_path)
                
                # Verify .dem file was extracted
                dem_files = glob.glob(os.path.join(demo_path, "**", "*.dem"), recursive=True)
                if not dem_files:
                    return False, f"No .dem file found in extracted content for {match_id}"
                
                return True, f"Successfully downloaded and extracted demo {match_id}"
            except zipfile.BadZipFile:
                return False, f"Downloaded file for {match_id} is not a valid zip file"

        except Exception as e:
            return False, f"Error downloading demo {match_id}: {str(e)}"

def download_user_demos(username, num_demos):
    """Download demos for a user"""
    try:
        # Check if matches file exists
        matches_file = f"{username}_matches.txt"
        if not os.path.exists(matches_file):
            return False, f"No matches file found for {username}"

        # Read match IDs (they're already in newest-first order)
        with open(matches_file, 'r') as f:
            match_ids = [line.strip() for line in f.readlines()]

        if not match_ids:
            return False, "No match IDs found"

        # Initialize downloader
        downloader = DemoDownloader()
        
        # Process matches (starting from newest)
        successful_downloads = 0
        skipped_downloads = 0
        results = []

        # Process matches until we have the requested number
        for match_id in match_ids:
            if successful_downloads >= num_demos:
                break

            success, message = downloader.download_demo(match_id, username)
            if success:
                successful_downloads += 1
                results.append(f"✓ {message}")
            elif "already exists" in message:
                skipped_downloads += 1
                results.append(f"⏭ {message}")
            else:
                results.append(f"✗ {message}")

            # If we've found enough demos (downloaded + existing), stop
            if successful_downloads + skipped_downloads >= num_demos:
                break

        # Get final quota info
        quota_info = downloader.get_quota_info()
        
        summary = (f"Downloaded: {successful_downloads}, "
                  f"Skipped: {skipped_downloads}, "
                  f"Total processed: {successful_downloads + skipped_downloads}")
        
        return True, {
            'summary': summary,
            'details': results,
            'quota_info': quota_info
        }

    except Exception as e:
        return False, f"Error in download process: {str(e)}"

def get_api_usage():
    """Get current API usage information"""
    try:
        downloader = DemoDownloader()
        response = requests.get(f"{downloader.base_url}/match/v2/match/1/stats", headers=downloader.headers)
        downloader.update_quota_info(response)
        return True, downloader.get_quota_info()
    except Exception as e:
        return False, f"Error getting API usage: {str(e)}"

if __name__ == "__main__":
    username = input("Enter FACEIT username: ")
    num_demos = int(input("Enter number of demos to download: "))
    success, result = download_user_demos(username)
    if success:
        print(result['summary'])
        print("\nQuota Information:")
        for key, value in result['quota_info'].items():
            print(f"{key}: {value}")
    else:
        print(f"Error: {result}")
