import os
import requests
import json
from pathlib import Path
import sys
from src.utils import unzip_file

# Define the base URL for Cricsheet data
data_url = "https://cricsheet.org/downloads/"

# Define base directory as the project root (one level above "src")
BASE_DIR = Path(__file__).resolve().parent.parent.parent  # Moves up one level from src
print(BASE_DIR)
DATA_DIR = BASE_DIR / "data"

# Define directories for storing data
RAW_DATA_DIR = DATA_DIR / "raw"
PROCESSED_DATA_DIR = DATA_DIR / "processed"

# Ensure directories exist
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DATA_DIR.mkdir(parents=True, exist_ok=True)

# List of match types to download
data_files = {
    "odis": "odis_json.zip"
}

def download_file(url, destination):
    """Downloads a file from a given URL and saves it to the destination path."""
    try:
        response = requests.get(url, stream=True)
        response.raise_for_status()
        
        with open(destination, "wb") as file:
            for chunk in response.iter_content(chunk_size=1024):
                if chunk:
                    file.write(chunk)
        
        print(f"Downloaded: {destination}")
    except requests.RequestException as e:
        print(f"Error downloading {url}: {e}")

def ingest_data():
    """Downloads and extracts cricket match data from Cricsheet."""
    for key, filename in data_files.items():
        url = f"{data_url}{filename}"
        zip_destination = RAW_DATA_DIR / filename
        
        if not zip_destination.exists():
            print(f"Downloading {key} data...")
            download_file(url, zip_destination)
        else:
            print(f"{key} data already exists. Skipping download.")
        
        return zip_destination

if __name__ == "__main__":
    
    zip_file_path = ingest_data()
    unzip_file(zip_file_path)
