import argparse
import json
import re
import sys
import time
from pathlib import Path
from typing import Any

import pandas as pd
import requests
import tqdm
from kaggle.api.kaggle_api_extended import KaggleApi

from dataset_scrapers.task_queue import TaskQueue


class MetadataDownloader:
    def __init__(
        self, data_dir: Path, output_dir: Path, max_pages: int = 100, num_workers: int = 1
    ) -> None:
        self.data_dir = data_dir
        self.output_dir = output_dir
        self.max_pages = max_pages
        self.max_workers = num_workers
        self.metadata_count = 0
        self.error_count = 0
        self.total_size = 0

        self.api = KaggleApi()
        self.api.authenticate()

    def start(self, keyword: str, start_index: int) -> None:
        if keyword:
            self.download_meta_kaggle_dataset()
            self.create_username_slug()
            refs = self.read_refs_from_file()
        else:
            refs = self.search_kaggle_datasets(keyword)

        self.total_size = len(refs)
        self.collect_metadata(start_index, refs)
        self.print_stats()

    def download_meta_kaggle_dataset(self) -> None:
        ref = "kaggle/meta-kaggle"
        if not (self.data_dir / "Datasets.csv").exists():
            print("Downloading Datasets.csv...")
            self.api.dataset_download_file(ref, file_name="Datasets.csv", path=self.data_dir)
        if not (self.data_dir / "DatasetVersions.csv").exists():
            print("Downloading DatasetVersions.csv...")
            self.api.dataset_download_file(
                ref, file_name="DatasetVersions.csv", path=self.data_dir
            )
        if not (self.data_dir / "Users.csv").exists():
            print("Downloading Users.csv...")
            self.api.dataset_download_file(ref, file_name="Users.csv", path=self.data_dir)

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
        merged = merged.dropna(subset=["UserName", "Slug"])
        merged = merged[["UserName", "Slug"]]
        with Path.open(self.data_dir / "username_slug.txt", "w") as file:
            for _, row in merged.iterrows():
                file.write(f"{row['UserName']}/{row['Slug']}\n")

    def search_kaggle_datasets(self, keyword: str) -> list[str]:
        refs = []
        try:
            for page in range(1, self.max_pages):
                datasets = self.api.dataset_list(search=keyword, page=page)
                if datasets is None:
                    continue
                refss = [data.ref for data in datasets if data is not None]
                if refss == []:
                    break
                refs.extend(refss)
        except Exception as e:  # noqa: BLE001
            print("Error while search Kaggle for a specific keyword:", e)
        return refs

    def read_refs_from_file(self) -> list[str]:
        try:
            with Path.open(self.data_dir / "username_slug.txt") as file:
                return [line.strip() for line in file]
        except Exception as e:  # noqa: BLE001
            print("Error while reading refs from file:", e)
            return []

    def get_croissant_metadata(self, ref: str) -> tuple[dict[str, Any] | Exception | int, int]:
        url = "https://www.kaggle.com/datasets/" + ref + "/croissant/download"
        response = requests.get(url, timeout=20)
        if response.status_code == 200:  # noqa: PLR2004
            try:
                result: dict[str, Any] = json.loads(response.content.decode("utf-8"))
            except Exception as e:  # noqa: BLE001
                return e, -2
            else:
                return result, 0
        elif response.status_code == 429:  # noqa: PLR2004
            return response.status_code, -1
        else:
            return response.status_code, -2

    def sanitize_filename(self, filename: str) -> str:
        return re.sub(r'[<>:"/\\|?*]', "_", filename)

    def save_metadata(self, metadata: dict[str, Any]) -> None:
        dirname = f"{metadata['kaggleRef']}"
        dirpath = self.output_dir / dirname
        dirpath.mkdir(exist_ok=True, parents=True)
        with Path.open(dirpath / "croissant_metadata.json", "w") as json_file:
            json.dump(metadata, json_file, indent=4)

    def process_ref(self, ref: str, progress: tqdm.tqdm) -> None:
        result, status = self.get_croissant_metadata(ref)
        if status == 0 and isinstance(result, dict):
            result["kaggleRef"] = ref
            self.save_metadata(result)
            self.metadata_count += 1
        elif status == -1:
            print(f"Got 'Too many requests' at progress {progress.n}, exiting ...")
            self.print_stats()
            sys.exit(0)
        else:
            with Path.open(self.data_dir / "error_datasets.txt", "a") as file:
                file.write(f"{ref},{result}\n")
            self.error_count += 1
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

    def print_stats(self) -> None:
        print(f"{self.metadata_count} metadata collected.")
        print(f"{self.error_count} errors occurred")
        print(f"{round(100 * self.metadata_count / self.total_size, 2)}% downloaded")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="download kaggle metadata")
    parser.add_argument(
        "--data-dir",
        type=str,
        default="../data",
        help="path to create a data directory (default %(default)s)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="../kaggle_metadata",
        help="path for kaggle metadata output directory (default %(default)s)",
    )
    parser.add_argument(
        "-k",
        "--keyword",
        type=str,
        default="",
        help="a specific keyword to search for (default %(default)s)",
    )
    parser.add_argument(
        "-i",
        "--start-index",
        type=int,
        default=0,
        help="start index to continue downloading (default %(default)s)",
    )
    parser.add_argument(
        "--max-pages",
        type=int,
        default=100,
        help="max number of result pages to search for a given keyword (default %(default)s)",
    )
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        help="number of parallel workers used to download metadata (default %(default)s)",
        default=1,
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    data_dir = Path(args.data_dir)
    output_dir = Path(args.output)
    data_dir.mkdir(exist_ok=True, parents=True)
    output_dir.mkdir(exist_ok=True, parents=True)

    downloader = MetadataDownloader(
        data_dir, output_dir, max_pages=args.max_pages, num_workers=args.workers
    )
    downloader.start(keyword=args.keyword, start_index=args.start_index)


if __name__ == "__main__":
    main()
