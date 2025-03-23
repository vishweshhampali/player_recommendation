import zipfile
from pathlib import Path


# Unzipping the file
def unzip_file(zip_path):
    """Unzips the given file into the specified directory."""
    try:
        if zip_path.exists():
            extract_to = zip_path.parent/"extracted/odi"
            if not extract_to.exists():
                print(f"Extracting data...")
                with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                    zip_ref.extractall(extract_to)
                    print(f"Extracted {zip_path} to {extract_to}")
            else:
                print(f"Extracted data already exists. Skipping extract.")
            
        else:
            print(f"File {zip_path} not found!")
        
    except zipfile.BadZipFile:
        print(f"Error: {zip_path} is not a valid zip file.")


