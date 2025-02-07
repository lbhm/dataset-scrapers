import argparse
import json
import os
import re
import time
from pathlib import Path

import pandas as pd
import requests
import tqdm
from kaggle.api.kaggle_api_extended import KaggleApi

from dataset_scrapers.task_queue import TaskQueue

os.chdir(os.path.dirname(os.path.abspath(__file__)))

# authentification
api = KaggleApi()
api.authenticate()

# TODO: make this configurable via command line arguments or a config file
MAX_WORKERS = 1
OUTPUT_DIR = Path("../kaggle_metadata")
METAKAGGLE_DIR = Path("../data")
MAX_PAGES = 100

metadata = 0
errors = 0
total_size = 0


def download_meta_kaggle_dataset():
    REF = "kaggle/meta-kaggle"
    METAKAGGLE_DIR.mkdir(exist_ok=True, parents=True)
    if not (METAKAGGLE_DIR / "Datasets.csv").exists():
        print("Downloading Datasets.csv...")
        api.dataset_download_file(REF, file_name="Datasets.csv", path=METAKAGGLE_DIR)
    if not (METAKAGGLE_DIR / "DatasetVersions.csv").exists():
        print("Downloading DatasetVersions.csv...")
        api.dataset_download_file(REF, file_name="DatasetVersions.csv", path=METAKAGGLE_DIR)
    if not (METAKAGGLE_DIR / "Users.csv").exists():
        print("Downloading Users.csv...")
        api.dataset_download_file(REF, file_name="Users.csv", path=METAKAGGLE_DIR)


def create_username_slug():
    if (METAKAGGLE_DIR / "username_slug.txt").exists():
        return
    print("Creating username_slug.txt...")
    username_slug = pd.read_csv(
        METAKAGGLE_DIR / "Datasets.csv",
        dtype={"CurrentDatasetVersionId": "Int64", "OwnerUserId": "Int64"},
    )
    username_slug = username_slug[
        ["Id", "CreatorUserId", "OwnerUserId", "CurrentDatasetVersionId"]
    ]
    username_slug = username_slug.rename(columns={"Id": "DatasetId"})

    dataset_versions = pd.read_csv(METAKAGGLE_DIR / "DatasetVersions.csv")
    dataset_versions = dataset_versions[["Id", "CreatorUserId", "VersionNumber", "Slug"]]
    dataset_versions = dataset_versions.rename(columns={"Id": "DatasetVersionId"})

    users = pd.read_csv(METAKAGGLE_DIR / "Users.csv")
    users = users[["Id", "UserName"]]
    users = users.rename(columns={"Id": "UserId"})

    merged = username_slug.merge(
        dataset_versions,
        how="left",
        left_on=["CurrentDatasetVersionId"],
        right_on=["DatasetVersionId"],
    ).merge(users, how="left", left_on="OwnerUserId", right_on="UserId")
    merged.dropna(subset=["UserName", "Slug"], inplace=True)
    merged = merged[["UserName", "Slug"]]
    with open(METAKAGGLE_DIR / "username_slug.txt", "w") as file:
        for _, row in merged.iterrows():
            file.write(f"{row['UserName']}/{row['Slug']}\n")
    print("finished!")


def search_kaggle_datasets(keyword: str) -> list[str]:
    refs = []
    try:
        for page in range(1, MAX_PAGES):  # MAX_PAGES
            datasets = api.dataset_list(search=keyword, page=page)
            refss = [data.ref for data in datasets]
            if refss == []:
                break
            refs.extend(refss)
        return refs
    except Exception as e:
        print("Ein Fehler ist aufgetreten:", e)
        return refs


def read_refs_from_file() -> list[str]:
    refs = []
    try:
        with open(METAKAGGLE_DIR / "username_slug.txt") as file:
            for line in file:
                refs.append(line.strip())
        return refs
    except Exception as e:
        print("Ein Fehler ist aufgetreten:", e)
        return refs


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
    return re.sub(r'[<>:"/\\|?*]', "_", filename)


def save_metadata(metadata):
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)
    dirname = f"{metadata['kaggleRef']}"
    dirpath = OUTPUT_DIR / dirname
    os.makedirs(dirpath, exist_ok=True)
    with open(dirpath / "croissant_metadata.json", "w") as json_file:
        json.dump(metadata, json_file, indent=4)


def process_ref(ref: str, progress: tqdm.tqdm):
    global metadata, errors
    result, status = get_croissant_metadata(ref)
    if status == 0:
        result["kaggleRef"] = ref
        save_metadata(result)
        metadata += 1
    elif status == -2:
        with open(METAKAGGLE_DIR / "error_datasets.txt", "a") as file:
            file.write(f"{ref},{result}\n")
        errors += 1
    else:
        print(f"Got 'Too many requests' at progress {progress.n}, exiting ...")
        finish_prints()
        os._exit(0)
    progress.update(1)


def collect_metadata(start_index: int, refs: list[str]):
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
    print(f"{round(100 * metadata / total_size, 2)}% downloaded")


def main():
    global total_size
    parser = argparse.ArgumentParser(description="download kaggle metadata (using a keyword)")
    parser.add_argument("--keyword", type=str, help="the keyword to search for", default="all")
    parser.add_argument("--index", type=int, help="start index to continue downloading", default=0)
    args = parser.parse_args()
    start_index = args.index
    keyword = args.keyword
    if keyword == "all":
        download_meta_kaggle_dataset()
        create_username_slug()
        refs = read_refs_from_file()
    else:
        refs = search_kaggle_datasets(keyword)
    total_size = len(refs)
    collect_metadata(start_index, refs)

    finish_prints()


if __name__ == "__main__":
    main()
