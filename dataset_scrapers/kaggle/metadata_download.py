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

class MetadataDownloader:
    def __init__(self, max_workers: int, max_pages: int, output_dir: Path, data_dir: Path) -> None:
        self.metadata = 0
        self.errors = 0
        self.total_size = 0
        self.max_workers = max_workers
        self.max_pages = max_pages
        self.output_dir = output_dir
        self.data_dir = data_dir

        # authentification
        api = KaggleApi()
        api.authenticate()
        self.api = api

    def start(self, keyword: str, start_index : int) -> None:
        if keyword == "all":
            self.download_meta_kaggle_dataset()
            self.create_username_slug()
            refs = self.read_refs_from_file()
        else:
            refs = self.search_kaggle_datasets(keyword)
        self.total_size = len(refs)
        self.collect_metadata(start_index, refs)
        self.finish_prints()


    def download_meta_kaggle_dataset(self) -> None:
        REF = "kaggle/meta-kaggle"
        if not (self.data_dir / "Datasets.csv").exists():
            print("Downloading Datasets.csv...")
            self.api.dataset_download_file(REF, file_name="Datasets.csv", path=self.data_dir)
        if not (self.data_dir / "DatasetVersions.csv").exists():
            print("Downloading DatasetVersions.csv...")
            self.api.dataset_download_file(REF, file_name="DatasetVersions.csv", path=self.data_dir)
        if not (self.data_dir / "Users.csv").exists():
            print("Downloading Users.csv...")
            self.api.dataset_download_file(REF, file_name="Users.csv", path=self.data_dir)


    def create_username_slug(self) -> None:
        if (self.data_dir / "username_slug.txt").exists():
            return
        print("Creating username_slug.txt...")
        username_slug = pd.read_csv(
            self.data_dir / "Datasets.csv",
            dtype={"CurrentDatasetVersionId": "Int64", "OwnerUserId": "Int64"},
        )
        username_slug = username_slug[
            ["Id", "CreatorUserId", "OwnerUserId", "CurrentDatasetVersionId"]
        ]
        username_slug = username_slug.rename(columns={"Id": "DatasetId"})

        dataset_versions = pd.read_csv(self.data_dir / "DatasetVersions.csv")
        dataset_versions = dataset_versions[["Id", "CreatorUserId", "VersionNumber", "Slug"]]
        dataset_versions = dataset_versions.rename(columns={"Id": "DatasetVersionId"})

        users = pd.read_csv(self.data_dir / "Users.csv")
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
        with open(self.data_dir / "username_slug.txt", "w") as file:
            for _, row in merged.iterrows():
                file.write(f"{row['UserName']}/{row['Slug']}\n")
        print("finished!")


    def search_kaggle_datasets(self, keyword: str) -> list[str]:
        refs = []
        try:
            for page in range(1, self.max_pages):  # MAX_PAGES
                datasets = self.api.dataset_list(search=keyword, page=page)
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


    def read_refs_from_file(self) -> list[str]:
        refs = []
        try:
            with open(self.data_dir / "username_slug.txt") as file:
                for line in file:
                    refs.append(line.strip())
            return refs
        except Exception as e:
            print("Ein Fehler ist aufgetreten:", e)
            return refs


    def get_croissant_metadata(self, ref: str) -> tuple[dict[str, Any] | Exception | int, int]:
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


    def sanitize_filename(self, filename: str) -> str:
        return re.sub(r'[<>:"/\\|?*]', "_", filename)


    def save_metadata(self, metadata: dict[str, Any]) -> None:
        dirname = f"{metadata['kaggleRef']}"
        dirpath = self.output_dir / dirname
        os.makedirs(dirpath, exist_ok=True)
        with open(dirpath / "croissant_metadata.json", "w") as json_file:
            json.dump(metadata, json_file, indent=4)


    def process_ref(self, ref: str, progress: tqdm.tqdm) -> None:
        result, status = self.get_croissant_metadata(ref)
        if status == 0 and isinstance(result, dict):
            result["kaggleRef"] = ref
            self.save_metadata(result)
            self.metadata += 1
        elif status == -2:
            with open(self.data_dir / "error_datasets.txt", "a") as file:
                file.write(f"{ref},{result}\n")
            self.errors += 1
        else:
            print(f"Got 'Too many requests' at progress {progress.n}, exiting ...")
            self.finish_prints()
            os._exit(0)
        progress.update(1)


    def collect_metadata(self, start_index: int, refs: list[str]) -> None:
        queue = TaskQueue(self.max_workers)

        with tqdm.tqdm(total=self.total_size, desc="Processing datasets") as progress:
            for ref in refs:
                if progress.n < start_index:
                    progress.update(1)
                    continue
                queue.add_task(self.process_ref, ref=ref, progress=progress)
                time.sleep(0.1)
            queue.join()


    def finish_prints(self) -> None:
        print(f"{self.metadata} metadata collected.")
        print(f"{self.errors} errors occurred")
        print(f"{round(100 * self.metadata / self.total_size, 2)}% downloaded")


def main() -> None:
    parser = argparse.ArgumentParser(description="download kaggle metadata (using a keyword)")
    parser.add_argument("--keyword", type=str, help="the keyword to search for", default="all")
    parser.add_argument("--index", type=int, help="start index to continue downloading", default=0)
    parser.add_argument("--maxpages", type=int, help="max pages to search for", default=100)
    parser.add_argument("--maxworkers", type=int, help="max workers used to download metadata", default=1)
    parser.add_argument("--data", type=str, help="path to create data dir", default="../data")
    parser.add_argument("--output", type=str, help="path for kaggle metadata output", default="../kaggle_metadata")
    args = parser.parse_args()

    keyword = args.keyword
    start_index = args.index
    MAX_PAGES = args.maxpages
    MAX_WORKERS = args.maxworkers
    DATA_DIR = Path(args.data)
    OUTPUT_DIR = Path(args.output)

    DATA_DIR.mkdir(exist_ok=True, parents=True)
    OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

    downloader = MetadataDownloader(MAX_WORKERS, MAX_PAGES, OUTPUT_DIR, DATA_DIR)
    downloader.start(keyword, start_index)
    

if __name__ == "__main__":
    main()
