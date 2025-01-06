from pathlib import Path
from kaggle.api.kaggle_api_extended import KaggleApi
import os
import tqdm

os.chdir(os.path.dirname(os.path.abspath(__file__)))

api = KaggleApi()
api.authenticate()

METADATA_DIR = "../kaggle_metadata" 

total_size = 0

def download_dataset(path: Path):
    download_dir = str(path.parent)
    ref = "/".join(download_dir.split("/")[2:])
    api.dataset_download_files(ref, path=download_dir, unzip=True)
    
def count_total():
    global total_size
    for path in Path(METADATA_DIR).rglob("*"):
        if path.is_file() and path.suffix == ".json":
            total_size += 1   

def main():
    count_total()
    with tqdm.tqdm(total=total_size, desc="downloading datasets") as progress:
        for path in Path(METADATA_DIR).rglob("*"):
            if path.is_file() and path.suffix == ".json":
                download_dataset(path)
                progress.update(1)  

if __name__ == "__main__":
    main()