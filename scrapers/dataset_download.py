from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import tqdm
import json
import argparse

os.chdir(os.path.dirname(os.path.abspath(__file__)))

api = KaggleApi()
api.authenticate()

unit_multipliers = {
        "B": 1/(1024**2),
        "KB": 1/1024,
        "MB": 1,
        "GB": 1024,
        "TB": 1024**2
    }

def convert_to_mb(string: str) -> float:
    parts = string.split()
    if len(parts) != 2:
        print(f"unexpected string format occurred: {string}")
        quit()
    return float(parts[0]) * unit_multipliers[parts[1]]

def conditions_fullfilled(path: Path) -> tuple[bool, float | None]:
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)  

    distribution = data["distribution"]
    for item in distribution:
        # contentSize indicates size of .zip download file
        if "contentSize" in item:
            size = item["contentSize"]  
            break
    size = convert_to_mb(size)
    
    # dataset too big (probably because of images)
    if size > 100:
        return False, None
    # dataset has no recordSet
    if "recordSet" not in data:
        return False, None
    
    return True, size

def download_dataset(path: Path):
    download_dir = str(path)
    ref = "/".join(download_dir.split("/")[2:])
    api.dataset_download_files(ref, path=download_dir, unzip=True)
    
def exists(target_path: Path, base_dir: Path) -> bool:
    for path in base_dir.rglob("*.csv"):
        if target_path != path and target_path.name == path.name:
            return True
    return False
    
def flatten_csv_folders(base_dir: Path):
    rename_list: list[tuple[Path, Path]] = []
    for path in base_dir.rglob("*.csv"):
        target_path = base_dir / path.name
        # skip if it is already flat
        if target_path == path:
            continue
        # resolve path conflicts
        if exists(path, base_dir):
            target_path = base_dir / str(path.relative_to(base_dir)).replace("/", "_")
        rename_list.append((path, target_path))
    # rename afterwards to avoid problems
    for src, target in rename_list:
        src.rename(target)
    # delete empty directories
    for folder in sorted(base_dir.rglob("*"), key=lambda p: -len(p.parts)): # start from the bottom
        if folder.is_dir() and not any(folder.iterdir()):
            folder.rmdir()
        
def main():
    parser = argparse.ArgumentParser(description="download kaggle datasets")
    parser.add_argument("--path", type=str, help="path to metadata", default="../kaggle_metadata")
    args = parser.parse_args()
    METADATA_DIR = Path(args.path)

    total_size = 0
    download_list : list[tuple[Path, int]] = []
    # create list of datasets to download
    for path in METADATA_DIR.rglob("metadata.json"):
        total_size += 1
        # filter datasets by conditions
        fulfilled, size = conditions_fullfilled(path)
        if fulfilled:
            download_list.append((path, size))

    # sort by size 
    download_list = sorted(download_list, key=lambda x: x[1])
    downloaded_size = len(download_list)
    # download datasets
    with tqdm.tqdm(total=downloaded_size, desc="downloading datasets") as progress:
        for path, size in download_list:
            download_dataset(path.parent)
            flatten_csv_folders(path.parent)
            progress.update(1)
    
    print(f"{downloaded_size} datasets downloaded ({round(downloaded_size / total_size * 100, 2)}%).")

if __name__ == "__main__":
    main()