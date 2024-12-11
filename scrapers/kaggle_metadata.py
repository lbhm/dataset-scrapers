import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from kaggle.api.kaggle_api_extended import KaggleApi
from kaggle.rest import ApiException
import tqdm
import os
import requests
import json
import re

# authentification
api = KaggleApi()
api.authenticate()

API_LIMIT = 200 # max requests per minute
MAX_PAGES = 10  # max pages
MAX_WORKERS = 3
OUTPUT_DIR = "../data"

def get_croissant_metadata(result):
    response_string = requests.get("https://www.kaggle.com/datasets/" + result.ref + "/croissant/download").content.decode("utf-8") 
    return json.loads(response_string)

def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '_', filename)


def fetch_page(page):
    try:
        results = api.dataset_list(
            sort_by="hottest",  
            file_type="csv",     
            license_name="all", 
            page=page,           
        )
        results = [get_croissant_metadata(result) for result in results]
        time.sleep(60 * MAX_WORKERS / API_LIMIT)
        return results
    except Exception as e:
        return {"error": e, "page": page}
    
def save_metadata(metadata):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # save results
    dir_metadata = os.path.join(OUTPUT_DIR, "kaggle_metadata")
    os.makedirs(dir_metadata, exist_ok=True)
    for m in metadata:
        filename = f"{sanitize_filename(m['name'])}.json"
        filepath = os.path.join(dir_metadata, filename)
        with open(filepath, "w") as json_file:
            json.dump(m, json_file, indent=4)

def collect_metadata():
    metadata_counter = 0
    errors = []
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        futures = {executor.submit(fetch_page, page): page for page in range(1, MAX_PAGES + 1)}
        for future in tqdm.tqdm(as_completed(futures), total=len(futures)):
            result = future.result()
            if isinstance(result, list):
                metadata_counter += len(result) # save success
                save_metadata(result)
            else:
                errors.append(result)  # save errors
    return metadata_counter, errors

def print_errors(errors):
    print(f"{len(errors)} errors occurred")
    if len(errors) == 0:
        return
    print("Error summmary:")
    for e in errors:
        print(f"Page {e['page']}: {e['error']}")

def main():
    print(f"collecting metadata for {MAX_PAGES} pages using {MAX_WORKERS} workers...")
    metadata_counter, errors = collect_metadata()
    print(f"{metadata_counter} metadata collected.")
    print_errors(errors)


if __name__ == "__main__":
    main()
