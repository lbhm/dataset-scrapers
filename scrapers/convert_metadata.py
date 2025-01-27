from pathlib import Path
import json

METADATA_DIR = Path("/local-ssd/lennart/data-collections/kaggle/kaggle_metadata")

def change_format(path: Path):
    dirpath = path.parent
    new_path = dirpath / "croissant_metadata.json"
    if (new_path).exists():
        return
    path.rename(new_path)




def main():
    for path in METADATA_DIR.rglob("metadata.json"):
        change_format(path)

if __name__ == "__main__":
    main()