import tqdm
import os
import requests
import json
import re
import time
from kaggle.api.kaggle_api_extended import KaggleApi
from TaskQueue import TaskQueue
import pandas as pd
import sys

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# authentification
api = KaggleApi()
api.authenticate()

MAX_WORKERS = 1
OUTPUT_DIR = "../kaggle_metadata"   
METAKAGGLE_DIR = "../data" 

metadata = 0
errors = 0

with open(os.path.join(METAKAGGLE_DIR, "username_slug.txt"), "r") as file:
    total_size = sum(1 for _ in file)

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
        
    queue = TaskQueue(MAX_WORKERS)
        
    with open(os.path.join(METAKAGGLE_DIR, "username_slug.txt"), "r") as file, tqdm.tqdm(total=total_size, desc="Processing datasets") as progress:
        for line in file:
            if progress.n < start_index:
                progress.update(1)
                continue
            ref = line.strip()
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
        
    download_meta_kaggle_dataset()
    create_username_slug()
    collect_metadata(start_index)
    
    finish_prints()
    
if __name__ == "__main__":
    main()
