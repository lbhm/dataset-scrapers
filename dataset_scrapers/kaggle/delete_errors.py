import shutil
from pathlib import Path


def main() -> None:
    error_path = Path("../error_list.log")
    fnf_paths: list[Path] = []
    # collect file not found paths
    with Path.open(error_path, "r") as f:
        for line in f:
            error = line.strip()
            if error.startswith("FileFileNotFoundError:"):
                path = error.split(":")[-1]
                fnf_paths.append(Path(path))
    # delete everything except croissant metadata
    for dataset_path in fnf_paths:
        for item in dataset_path.rglob("*"):
            if item.name == "croissant_metadata.json":
                continue
            if item.is_file():
                item.unlink()
            if item.is_dir():
                shutil.rmtree(item)
    print(f"{len(fnf_paths)} datasets with file not found erros deleted!")


if __name__ == "__main__":
    main()
