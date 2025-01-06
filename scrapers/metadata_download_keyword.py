from concurrent.futures import ThreadPoolExecutor
from multiprocessing import process
from queue import Queue
import tqdm
import os
import requests
import json
import re
import time
import subprocess
from kaggle.api.kaggle_api_extended import KaggleApi
from TaskQueue import TaskQueue
import pandas as pd
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# authentification
api = KaggleApi()
api.authenticate()

MAX_WORKERS = 1
MAX_PAGES = 100
OUTPUT_DIR = "../kaggle_metadata"   
METAKAGGLE_DIR = "../data"

refs = [] 
total_size = 0

metadata = 0
errors = 0

def search_kaggle_datasets(keyword):
    global refs, total_size
    try:
        for page in range(1, MAX_PAGES):
            datasets = api.dataset_list(search=keyword, page=page)
            refss = [data.ref for data in datasets]
            if refss == []:
                break
            refs.extend(refss)
    except Exception as e:
        print("Ein Fehler ist aufgetreten:", e)
        return
    total_size = len(refs)

def get_croissant_metadata(ref):
    url = "https://www.kaggle.com/datasets/" + ref + "/croissant/download"
    response = requests.get(url)
    if response.status_code == 200:
        try:
            result = json.loads(response.content.decode("utf-8"))
            return result, 0
        except Exception as e:
            return e, -2
            
    elif response.status_code == 429:
        return response.status_code, -1
    else:
        return response.status_code, -2

def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def save_metadata(metadata):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    dirname = f"{metadata['ref']}"
    dirpath = os.path.join(OUTPUT_DIR, dirname)
    os.makedirs(dirpath, exist_ok=True)
    with open(os.path.join(dirpath, "metadata.json"), "w") as json_file:
        json.dump(metadata, json_file, indent=4)

def process_ref(ref: str, progress: tqdm.tqdm):
    global metadata, errors
    result, status = get_croissant_metadata(ref)
    if status == 0:
        save_metadata({"ref": ref, "jsonld": result})
        metadata += 1
    elif status == -2:
        with open(os.path.join(METAKAGGLE_DIR, "error_datasets.txt"), "a") as file:
            file.write(f"{ref},{result}\n") 
        errors += 1
    else:
        print(f"Got 'Too many requests' at progress {progress.n}, exiting ...")
        finish_prints()
        os._exit(0)
    progress.update(1)
    

def collect_metadata(start_index: int):  
    global refs
    queue = TaskQueue(MAX_WORKERS)    
    with tqdm.tqdm(total=total_size, desc="Processing datasets") as progress:
        for ref in refs:
            if progress.n < start_index:
                progress.update(1)
                continue
            queue.add_task(process_ref, ref=ref, progress=progress)
            time.sleep(0.1)
        queue.join()
        
def finish_prints():
    print(f"{metadata} metadata collected.")
    print(f"{errors} errors occurred")
    print(f"{round(100 * metadata/total_size, 2)}% downloaded")
    

def main():
    if len(sys.argv) < 2:
        print("No index provided. Setting start index to 0")
        start_index = 0
    else:
        start_index = int(sys.argv[1])
        print(f"Using {start_index} as start index")
    search_kaggle_datasets("lung cancer")
    collect_metadata(start_index) 
    finish_prints()
    
if __name__ == "__main__":
    main()
