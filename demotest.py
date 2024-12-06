import os
import requests
from urllib.parse import urlparse, parse_qs

# Constants
API_KEY = '258bb0ff-65c0-4145-ba7d-d9617986361b'
MATCH_ID = '1-c9709bdf-c72f-4b74-9580-7184bb393d22'
DEMO_URL = 'https://demos-us-east.backblaze.faceit-cdn.net/cs2/1-c9709bdf-c72f-4b74-9580-7184bb393d22-1-1.dem.gz'
DOWNLOAD_API_URL = 'https://open.faceit.com/download/v2/demos/download'
SAVE_FOLDER = r'C:\demofetch\demo_test'
SAVE_FILE_NAME = f'{MATCH_ID}.dem.gz'

# Ensure save folder exists
os.makedirs(SAVE_FOLDER, exist_ok=True)

# Get the signed URL
headers = {
    'Authorization': f'Bearer {API_KEY}',
    'Content-Type': 'application/json'
}
payload = {
    'resource_url': DEMO_URL
}

response = requests.post(DOWNLOAD_API_URL, headers=headers, json=payload)

if response.status_code == 200:
    signed_url = response.json().get('payload', {}).get('download_url', '')
    if signed_url:
        # Check if the URL appears to be signed
        parsed_url = urlparse(signed_url)
        query_params = parse_qs(parsed_url.query)

        if 'Signature' in query_params or 'Expires' in query_params or 'AWSAccessKeyId' in query_params:
            print(f"Signed URL received: {signed_url}")
        else:
            print(f"URL received, but it doesn't look signed: {signed_url}")

        # Download the demo file
        print("Downloading the demo file...")
        demo_response = requests.get(signed_url, stream=True)
        if demo_response.status_code == 200:
            save_path = os.path.join(SAVE_FOLDER, SAVE_FILE_NAME)
            with open(save_path, 'wb') as demo_file:
                for chunk in demo_response.iter_content(chunk_size=8192):
                    demo_file.write(chunk)
            print(f"Demo file saved to {save_path}")
        else:
            print(f"Failed to download the demo file. Status code: {demo_response.status_code}")
    else:
        print("No signed URL received in the response.")
else:
    print(f"Failed to get the signed URL. Status code: {response.status_code}, Response: {response.text}")
