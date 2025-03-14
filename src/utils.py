import zipfile
from pathlib import Path


# Unzipping the file
def unzip_file(zip_path):
    """Unzips the given file into the specified directory."""
    try:
        extract_to = zip_path.parent/"extracted"
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            zip_ref.extractall(extract_to)
        print(f"Extracted {zip_path} to {extract_to}")
    except zipfile.BadZipFile:
        print(f"Error: {zip_path} is not a valid zip file.")


