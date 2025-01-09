from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import tqdm
import json
import shutil

os.chdir(os.path.dirname(os.path.abspath(__file__)))

api = KaggleApi()
api.authenticate()

METADATA_DIR = "../kaggle_metadata" 

total_size = 0
downloaded_size = 0

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

def conditions_fullfilled(path: Path) -> bool:
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)  

    distribution = data["jsonld"]["distribution"]
    for item in distribution:
        # contentSize indicates size of .zip download file
        if "contentSize" in item:
            size = item["contentSize"]  
            break
    
    # dataset too big (probably because of images)
    if convert_to_mb(size) > 100:
        return False
    # dataset has no recordSet
    if "recordSet" not in data["jsonld"]:
        return False
    
    return True

def download_dataset(path: Path):
    download_dir = str(path.parent)
    ref = "/".join(download_dir.split("/")[2:])
    api.dataset_download_files(ref, path=download_dir, unzip=True)
    
def count_total():
    global total_size
    for path in Path(METADATA_DIR).rglob("*"):
        if path.is_file() and str(path).endswith("metadata.json"):
            total_size += 1   

def main():
    global downloaded_size
    count_total()
    with tqdm.tqdm(total=total_size, desc="downloading datasets") as progress:
        for path in Path(METADATA_DIR).rglob("*"):
            if path.is_file() and str(path).endswith("metadata.json"):
                if conditions_fullfilled(path):
                    download_dataset(path)
                    downloaded_size += 1
                else:
                    shutil.rmtree(path.parent)
                    if not any(path.parent.parent.iterdir()):
                        shutil.rmtree(path.parent.parent)
                    print(f"removed {path.parent}") 
                progress.update(1) 

    print(f"{downloaded_size} datasets downloaded ({round(downloaded_size / total_size * 100, 2)}%).")

if __name__ == "__main__":
    main()