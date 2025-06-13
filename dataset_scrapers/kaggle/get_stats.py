import json
from pathlib import Path
from typing import Any


def get_stats_croissant(metadata: dict[str, Any]) -> tuple[int, int]:
    distribution = metadata["distribution"]
    records = metadata["recordSet"]
    file_count = len([item for item in distribution if "csv" in item["@id"]])
    column_count = len([column for file_record in records for column in file_record["field"]])
    return file_count, column_count


def get_stats_kaggle() -> None:
    croissant_path = Path("../croissant")
    kaggle_path = Path("../kaggle_metadata")

    dataset_paths = [
        path.parent
        for path in kaggle_path.rglob("croissant_metadata.json")
        if len(list(path.parent.iterdir())) > 1
    ]

    total_croissant_file_count = 0
    total_croissant_column_count = 0
    total_kaggle_file_count = 0
    total_kaggle_column_count = 0

    for path in dataset_paths:
        kaggle_metadata_path = path / "croissant_metadata.json"
        file_name = "/".join(str(path).split("/")[-2:]).replace("/", "_") + ".json"
        croissant_metadata_path = croissant_path / file_name
        if not kaggle_metadata_path.exists():
            continue

        with kaggle_metadata_path.open("r", encoding="utf-8") as file:
            metadata = json.load(file)

        kaggle_file_count, kaggle_column_count = get_stats_croissant(metadata)
        total_kaggle_file_count += kaggle_file_count
        total_kaggle_column_count += kaggle_column_count

        with croissant_metadata_path.open("r", encoding="utf-8") as file:
            croissant_metadata = json.load(file)

        croissant_file_count, croissant_column_count = get_stats_croissant(croissant_metadata)
        total_croissant_file_count += croissant_file_count
        total_croissant_column_count += croissant_column_count

        print(
            f"{file_name}: "
            f"Files difference: {croissant_file_count - kaggle_file_count}, "
            f"Columns difference: {croissant_column_count - kaggle_column_count}"
        )

    print(
        f"Total Kaggle - Files: {total_kaggle_file_count}, Columns: {total_kaggle_column_count} | "
        f"Total Croissant - Files: {total_croissant_file_count}, Columns: {total_croissant_column_count}"  # noqa: E501
    )


if __name__ == "__main__":
    get_stats_kaggle()
    print("Statistics collection completed.")
