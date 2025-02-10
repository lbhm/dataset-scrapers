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
from typing import Any

MAX_WORKERS = 1
MAX_PAGES = 100
OUTPUT_DIR = Path("../kaggle_metadata")
DATA_DIR = Path("../data")

metadata = 0
errors = 0
total_size = 0


def download_meta_kaggle_dataset(api: KaggleApi) -> None:
    REF = "kaggle/meta-kaggle"
    DATA_DIR.mkdir(exist_ok=True, parents=True)
    if not (DATA_DIR / "Datasets.csv").exists():
        print("Downloading Datasets.csv...")
        api.dataset_download_file(REF, file_name="Datasets.csv", path=DATA_DIR)
    if not (DATA_DIR / "DatasetVersions.csv").exists():
        print("Downloading DatasetVersions.csv...")
        api.dataset_download_file(REF, file_name="DatasetVersions.csv", path=DATA_DIR)
    if not (DATA_DIR / "Users.csv").exists():
        print("Downloading Users.csv...")
        api.dataset_download_file(REF, file_name="Users.csv", path=DATA_DIR)


def create_username_slug() -> None:
    if (DATA_DIR / "username_slug.txt").exists():
        return
    print("Creating username_slug.txt...")
    username_slug = pd.read_csv(
        DATA_DIR / "Datasets.csv",
        dtype={"CurrentDatasetVersionId": "Int64", "OwnerUserId": "Int64"},
    )
    username_slug = username_slug[
        ["Id", "CreatorUserId", "OwnerUserId", "CurrentDatasetVersionId"]
    ]
    username_slug = username_slug.rename(columns={"Id": "DatasetId"})

    dataset_versions = pd.read_csv(DATA_DIR / "DatasetVersions.csv")
    dataset_versions = dataset_versions[["Id", "CreatorUserId", "VersionNumber", "Slug"]]
    dataset_versions = dataset_versions.rename(columns={"Id": "DatasetVersionId"})

    users = pd.read_csv(DATA_DIR / "Users.csv")
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
    with open(DATA_DIR / "username_slug.txt", "w") as file:
        for _, row in merged.iterrows():
            file.write(f"{row['UserName']}/{row['Slug']}\n")
    print("finished!")


def search_kaggle_datasets(api: KaggleApi, keyword: str) -> list[str]:
    refs = []
    try:
        for page in range(1, MAX_PAGES):  # MAX_PAGES
            datasets = api.dataset_list(search=keyword, page=page)
            if datasets is None:
                continue
            refss = [data.ref for data in datasets if data is not None]
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
        with open(DATA_DIR / "username_slug.txt") as file:
            for line in file:
                refs.append(line.strip())
        return refs
    except Exception as e:
        print("Ein Fehler ist aufgetreten:", e)
        return refs


def get_croissant_metadata(ref: str) -> tuple[dict[str, Any] | Exception | int, int]:
    url = "https://www.kaggle.com/datasets/" + ref + "/croissant/download"
    response = requests.get(url)
    if response.status_code == 200:
        try:
            result: dict[str, Any] = json.loads(response.content.decode("utf-8"))
            return result, 0
        except Exception as e:
            return e, -2

    elif response.status_code == 429:
        return response.status_code, -1
    else:
        return response.status_code, -2


def sanitize_filename(filename: str) -> str:
    return re.sub(r'[<>:"/\\|?*]', "_", filename)


def save_metadata(metadata: dict[str, Any]) -> None:
    dirname = f"{metadata['kaggleRef']}"
    dirpath = OUTPUT_DIR / dirname
    os.makedirs(dirpath, exist_ok=True)
    with open(dirpath / "croissant_metadata.json", "w") as json_file:
        json.dump(metadata, json_file, indent=4)


def process_ref(ref: str, progress: tqdm.tqdm) -> None:
    global metadata, errors
    result, status = get_croissant_metadata(ref)
    if status == 0 and isinstance(result, dict):
        result["kaggleRef"] = ref
        save_metadata(result)
        metadata += 1
    elif status == -2:
        with open(DATA_DIR / "error_datasets.txt", "a") as file:
            file.write(f"{ref},{result}\n")
        errors += 1
    else:
        print(f"Got 'Too many requests' at progress {progress.n}, exiting ...")
        finish_prints()
        os._exit(0)
    progress.update(1)


def collect_metadata(start_index: int, refs: list[str]) -> None:
    queue = TaskQueue(MAX_WORKERS)

    with tqdm.tqdm(total=total_size, desc="Processing datasets") as progress:
        for ref in refs:
            if progress.n < start_index:
                progress.update(1)
                continue
            queue.add_task(process_ref, ref=ref, progress=progress)
            time.sleep(0.1)
        queue.join()


def finish_prints() -> None:
    print(f"{metadata} metadata collected.")
    print(f"{errors} errors occurred")
    print(f"{round(100 * metadata / total_size, 2)}% downloaded")


def main() -> None:
    global total_size, MAX_PAGES, MAX_WORKERS, DATA_DIR, OUTPUT_DIR
    # authentification
    api = KaggleApi()
    api.authenticate()

    parser = argparse.ArgumentParser(description="download kaggle metadata (using a keyword)")
    parser.add_argument("--keyword", type=str, help="the keyword to search for", default="all")
    parser.add_argument("--index", type=int, help="start index to continue downloading", default=0)
    parser.add_argument("--maxpages", type=int, help="max pages to search for", default=100)
    parser.add_argument("--maxworkers", type=int, help="max workers used to download metadata", default=1)
    parser.add_argument("--data", type=str, help="path to create data dir", default="../data")
    parser.add_argument("--output", type=str, help="path for kaggle metadata output", default="../kaggle_metadata")
    args = parser.parse_args()

    start_index = args.index
    keyword = args.keyword
    MAX_PAGES = args.maxpages
    MAX_WORKERS = args.maxworkers
    if Path(args.data).exists():
        DATA_DIR = Path(args.data)
    if Path(args.output).exists():
        OUTPUT_DIR = Path(args.output)
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

    if keyword == "all":
        download_meta_kaggle_dataset(api)
        create_username_slug()
        refs = read_refs_from_file()
    else:
        refs = search_kaggle_datasets(api, keyword)
    total_size = len(refs)
    collect_metadata(start_index, refs)

    finish_prints()


if __name__ == "__main__":
    main()
