from concurrent.futures import ThreadPoolExecutor
from multiprocessing import process
from queue import Queue
import tqdm
import os
import requests
import json
import re
import time
from kaggle.api.kaggle_api_extended import KaggleApi
from TaskQueue import TaskQueue
import pandas as pd

# authentification
api = KaggleApi()
api.authenticate()

MAX_WORKERS = 2
OUTPUT_DIR = "../kaggle_metadata"   
METAKAGGLE_DIR = "../data" 

metadata = 0
errors = 0

def download_meta_kaggle_dataset():
    REF = "kaggle/meta-kaggle"
    os.makedirs(METAKAGGLE_DIR, exist_ok=True)
    if not os.path.exists(os.path.join(METAKAGGLE_DIR, "Datasets.csv")):
        print("Downloading Datasets.csv...")
        api.dataset_download_file(REF, file_name="Datasets.csv", path=METAKAGGLE_DIR)
    if not os.path.exists(os.path.join(METAKAGGLE_DIR, "DatasetVersions.csv")):
        print("Downloading DatasetVersions.csv...")
        api.dataset_download_file(REF, file_name="DatasetVersions.csv", path=METAKAGGLE_DIR)
    if not os.path.exists(os.path.join(METAKAGGLE_DIR, "Users.csv")):
        print("Downloading Users.csv...")
        api.dataset_download_file(REF, file_name="Users.csv", path=METAKAGGLE_DIR)

def create_username_slug():
    if os.path.exists(os.path.join(METAKAGGLE_DIR, "username_slug.txt")):
        return
    print("Creating username_slug.txt...")
    username_slug = pd.read_csv(os.path.join(METAKAGGLE_DIR, "Datasets.csv"), dtype={"CurrentDatasetVersionId": "Int64", "OwnerUserId": "Int64"})
    username_slug = username_slug[["Id", "CreatorUserId", "OwnerUserId", "CurrentDatasetVersionId"]]
    username_slug = username_slug.rename(columns={"Id": "DatasetId"})
    
    dataset_versions = pd.read_csv(os.path.join(METAKAGGLE_DIR, "DatasetVersions.csv"))
    dataset_versions = dataset_versions[["Id", "CreatorUserId", "VersionNumber", "Slug"]]
    dataset_versions = dataset_versions.rename(columns={"Id": "DatasetVersionId"})
    
    users = pd.read_csv(os.path.join(METAKAGGLE_DIR, "Users.csv"))
    users = users[["Id", "UserName"]]
    users = users.rename(columns={"Id": "UserId"})
    
    merged = username_slug.merge(dataset_versions, how="left", left_on=["CurrentDatasetVersionId"], right_on=["DatasetVersionId"],).merge(users, how="left", left_on="OwnerUserId", right_on="UserId")
    merged.dropna(subset=["UserName", "Slug"], inplace=True)
    merged = merged[["UserName", "Slug"]]
    with open(os.path.join(METAKAGGLE_DIR, "username_slug.txt"), "w") as file:
        for _, row in merged.iterrows():
            file.write(f"{row['UserName']}/{row['Slug']}\n") 
    print("finished!")

def get_croissant_metadata(ref):
    url = "https://www.kaggle.com/datasets/" + ref + "/croissant/download"
    response_string = requests.get(url).content.decode("utf-8")
    return json.loads(response_string)

def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def save_metadata(metadata):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    dirname = f"{sanitize_filename(metadata['jsonld']['name'])}"
    dirpath = os.path.join(OUTPUT_DIR, dirname)
    os.makedirs(dirpath, exist_ok=True)
    with open(os.path.join(dirpath, dirname + ".json"), "w") as json_file:
        json.dump(metadata, json_file, indent=4)

def process_ref(ref: str, progress: tqdm.tqdm):
    global metadata, errors
    try:
        croissant_sample = get_croissant_metadata(ref)
        save_metadata({"ref": ref, "jsonld": croissant_sample})
        metadata += 1
    except Exception as e:
        with open(os.path.join(METAKAGGLE_DIR, "error_datasets.txt"), "a") as file:
            file.write(f"{ref},{e}\n") 
        errors += 1
    progress.update(1)
    

def collect_metadata():  
    with open(os.path.join(METAKAGGLE_DIR, "username_slug.txt"), "r") as file:
        total_lines = sum(1 for _ in file)
        
    queue = TaskQueue(MAX_WORKERS)
        
    with open(os.path.join(METAKAGGLE_DIR, "username_slug.txt"), "r") as file, tqdm.tqdm(total=total_lines, desc="Processing datasets") as progress:
        for counter, line in enumerate(file):
            ref = line.strip()
            queue.add_task(process_ref, ref=ref, progress=progress)
            time.sleep(0.1)
            if counter > 1000:
                break
        queue.join()
    

def main():
    download_meta_kaggle_dataset()
    create_username_slug()
    collect_metadata()
    print(f"{metadata} metadata collected.")
    print(f"{errors} errors occurred")
    print(f"{round(100 * metadata/(metadata+errors), 2)}% success rate")
    
if __name__ == "__main__":
    main()
