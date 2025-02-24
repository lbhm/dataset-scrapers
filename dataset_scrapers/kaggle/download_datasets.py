import argparse
import contextlib
import json
import shutil
import sys
from collections.abc import Generator
from pathlib import Path
from typing import TextIO

import tqdm
from kaggle.api.kaggle_api_extended import KaggleApi
from tqdm.contrib import DummyTqdmFile


@contextlib.contextmanager
def redirect_to_tqdm() -> Generator[TextIO]:
    orig_out_err = sys.stdout, sys.stderr
    try:
        sys.stdout, sys.stderr = map(DummyTqdmFile, orig_out_err)
        yield orig_out_err[0]
    except Exception as exc:
        raise exc
    finally:
        sys.stdout, sys.stderr = orig_out_err


class DatasetDownloader:
    def __init__(self, metadata_dir: Path, start_index: int = 0) -> None:
        self.metadata_dir = metadata_dir
        self.start_index = start_index
        self.total_size = 0

        self.api = KaggleApi()
        self.api.authenticate()

        # NOTE: This could be moved to a utils file
        self.unit_multipliers = {
            "B": 1 / (1024**2),
            "KB": 1 / 1024,
            "MB": 1,
            "GB": 1024,
            "TB": 1024**2,
        }

    def convert_to_mb(self, file_size: str) -> float:
        parts = file_size.split()
        if len(parts) != 2 or parts[1] not in self.unit_multipliers:
            raise ValueError(f"Unexpected file size format occurred: {file_size}")

        return float(parts[0]) * self.unit_multipliers[parts[1]]

    def conditions_fullfilled(
        self, path: Path, max_size: float = 100.0
    ) -> tuple[bool, float | None]:
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)

        distribution = data["distribution"]
        size_string = "0 B"
        for item in distribution:
            # contentSize indicates size of .zip download file
            if "contentSize" in item:
                size_string = item["contentSize"]
                break
        size = self.convert_to_mb(size_string)

        # NOTE: We skip very large datasets
        if size > max_size or "recordSet" not in data:
            return False, None

        return True, size

    def download_dataset(self, path: Path) -> None:
        ref = "/".join(path.parts[-2:])
        self.api.dataset_download_files(ref, path=str(path), unzip=True)

    def exists(self, target_path: Path, base_dir: Path) -> bool:
        for path in base_dir.rglob("*.csv"):
            if target_path != path and target_path.name == path.name:
                return True
        return False

    def flatten_csv_folders(self, base_dir: Path) -> None:  # noqa: C901
        # remove single subfolder if exists
        subfiles = [f for f in base_dir.iterdir() if f.name != "croissant_metadata.json"]
        if len(subfiles) == 1 and subfiles[0].is_dir():
            subfolder = subfiles[0]
            for item in subfolder.iterdir():
                # remove duplicate subfolder
                if item.name == subfolder.name and item.is_dir():
                    shutil.rmtree(item)
                else:
                    new_path = base_dir / item.name
                    # avoid conflicts if directory has .csv ending
                    if new_path.exists() and new_path.is_dir():
                        new_path.rename(new_path.with_name(new_path.stem))
                    item.rename(new_path)
            subfolder.rmdir()

        rename_list: list[tuple[Path, Path]] = []
        for path in base_dir.rglob("*.csv"):
            if not path.is_file():
                continue
            target_path = base_dir / path.name
            # skip if it is already flat
            if target_path == path:
                continue
            # resolve path conflicts
            if self.exists(path, base_dir):
                target_path = base_dir / str(path.relative_to(base_dir)).replace("/", "_")
            rename_list.append((path, target_path))
        # rename afterwards to avoid problems
        for src, target in rename_list:
            src.rename(target)
        # delete empty directories
        for folder in sorted(
            base_dir.rglob("*"), key=lambda p: -len(p.parts)
        ):  # start from the bottom
            if folder.is_dir() and not any(folder.iterdir()):
                folder.rmdir()

    def start(self) -> None:
        download_list: list[tuple[Path, float]] = []
        # create list of datasets to download
        for path in self.metadata_dir.rglob("croissant_metadata.json"):
            self.total_size += 1
            # filter datasets by conditions
            try:
                fulfilled, size = self.conditions_fullfilled(path)
            except Exception as e:
                print(f"Exception occurred with {path}: {e}")
                sys.exit(1)
            if fulfilled and size is not None:
                download_list.append((path, size))

        # sort by size
        download_list = sorted(download_list, key=lambda x: x[1])
        n_downloads = len(download_list)
        # download datasets
        with (
            redirect_to_tqdm() as output,
            tqdm.tqdm(total=n_downloads, desc="Downloading datasets", file=output) as progress,
        ):
            for path, _ in download_list:
                # skip datasets before start_index
                if progress.n < self.start_index:
                    progress.update(1)
                    continue
                # check if dataset is already downloaded
                if len(list(path.parent.iterdir())) > 1:
                    continue
                try:
                    self.download_dataset(path.parent)
                except Exception as e:
                    print(f"Exception occurred with {path}: {e}")
                self.flatten_csv_folders(path.parent)
                progress.update(1)

        print(
            f"{n_downloads} datasets downloaded or cached "
            f"({round(n_downloads / self.total_size * 100, 2)}%)."
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="download kaggle datasets")
    parser.add_argument(
        "--path",
        type=str,
        default="../kaggle_metadata",
        help="path to metadata (default %(default)s)",
    )
    parser.add_argument(
        "-i",
        "--start-index",
        type=int,
        default=0,
        help="start index to continue downloading (default %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    metadata_dir = Path(args.path)

    if not metadata_dir.exists():
        print("This program requires a directory with croissant metadata to work!")
        sys.exit(1)

    downloader = DatasetDownloader(metadata_dir, start_index=args.start_index)
    downloader.start()


if __name__ == "__main__":
    main()
