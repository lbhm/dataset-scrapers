from contextlib import redirect_stderr
import tqdm
import os
import requests
import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from kaggle.api.kaggle_api_extended import KaggleApi, ApiException
from io import StringIO
#import mlcroissant as mlc # causes NoneType error in the end
 
# authentification
api = KaggleApi()
api.authenticate()

API_LIMIT = 200 # max requests per minute
MAX_PAGES = 2  # max pages
MAX_WORKERS = 1
MAX_SIZE = 100000 # max datasets size in bytes
OUTPUT_DIR = "../kaggle_datasets"

# transform URIRef("https://...") keys
def edit_json_data(json: dict):
    new_json = {}
    for key, value in json.items():
        string_key = str(key)
        last_slash_index = string_key.rfind('/')
        if last_slash_index == -1:
            new_json[key] = value
        else: 
            new_json[string_key[last_slash_index + 1:]] = value
    return new_json
    

def get_croissant_metadata(ref):
    url = "https://www.kaggle.com/datasets/" + ref + "/croissant/download"
    response_string = requests.get(url).content.decode("utf-8") 
    return json.loads(response_string)
    #fnull = StringIO()
    #with redirect_stderr(fnull): # hide annoying err prints
    #    response = mlc.Dataset(url).metadata.jsonld
    #return edit_json_data(response)


def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '_', filename)


def fetch_page(page):
    try:
        results = api.dataset_list(
            sort_by="hottest",    
            file_type="csv",     
            license_name="all", 
            page=page,
            max_size=MAX_SIZE
        )
        metadata = [{"ref": result.ref, "jsonld": get_croissant_metadata(result.ref)} for result in results]
        time.sleep(60 * MAX_WORKERS / API_LIMIT)
        return metadata
    except ApiException as e:
        return {"error": e, "page": page}
    
def save_metadata(metadata):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    for m in metadata:
        dirname = f"{sanitize_filename(m['jsonld']['name'])}"
        dirpath = os.path.join(OUTPUT_DIR, dirname)
        os.makedirs(dirpath, exist_ok=True)
        with open(os.path.join(dirpath, dirname + ".json"), "w") as json_file:
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
