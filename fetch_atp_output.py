import requests
import json
from pathlib import Path


url = "https://data.alltheplaces.xyz/runs/history.json"
output_file = Path("runs.json")
if not output_file.exists(): 
    with open(output_file, 'x') as file: 
        file.write("[]") 

def fetch_data():
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data. Status code: {response.status_code}")
        return None

def download_file(url):
    local_filename = url.split('/')[-1]
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(local_filename, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)
    return local_filename


data = fetch_data()
if data:
    # Process the data (e.g., extract the last run_id)
    last_run_id = data[-1]["run_id"]
    output_zip_url = data[-1]["output_url"]
    # Check if the run_id is already in the local file
    with open(output_file, "r+") as f:
        existing_runs = json.load(f)
        if last_run_id not in existing_runs:
            # Add the new run_id to the local array
            existing_runs.append(last_run_id)
            download_file(output_zip_url)
            with open(output_file, "w") as f:
                json.dump(existing_runs, f, indent=2)
            print(f"Added run_id {last_run_id} to {output_file}")
