from pathlib import Path
import json

METADATA_DIR = Path("/local-ssd/lennart/data-collections/kaggle/kaggle_metadata")

def change_format(path: Path):
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)

    if "ref" not in data:
        return
    
    ref = data["ref"]
    data = data["jsonld"]
    data["kaggleRef"] = ref
    with path.open("w") as f:
        json.dump(data, f, indent=4, ensure_ascii=False)




def main():
    for path in METADATA_DIR.rglob("metadata.json"):
        change_format(path)

if __name__ == "__main__":
    main()