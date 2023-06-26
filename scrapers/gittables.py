import io
import json
import os
import zipfile

import numpy as np
import pandas as pd
import requests
from pyarrow import parquet as pq
from tqdm import tqdm


def download_and_unzip(r_id: int, path: str):
    response = requests.get(f"https://zenodo.org/api/records/{r_id}")
    if response.status_code == 200:
        record_info = response.json()
        files = record_info["files"]
        fails = []
        for file_info in tqdm(files):
            download_url = file_info["links"]["self"]
            response = requests.get(download_url)
            if response.status_code == 200:
                if zipfile.is_zipfile(io.BytesIO(response.content)):
                    with zipfile.ZipFile(io.BytesIO(response.content), "r") as zip_file:
                        extract_path = os.path.join(
                            path, "parquets", file_info["key"][:-4]
                        )
                        os.makedirs(extract_path, exist_ok=True)
                        zip_file.extractall(extract_path)
            else:
                fails.append(file_info["key"])
        if len(fails) != 0:
            print(f"Failed to download files: {fails}")
    else:
        print(f"HTTP request failed. response code: {response.status_code}")


def create_profiles(path: str, additional_data: list):
    parquet_dir = os.path.join(path, "parquets")
    json_dir = os.path.join(path, "jsons")
    os.makedirs(json_dir, exist_ok=True)
    json_fails = []
    additional_fails = []
    for sub_dir in tqdm(os.listdir(parquet_dir), position=1, leave=True):
        for filename in tqdm(
            os.listdir(os.path.join(parquet_dir, sub_dir)), position=0, leave=True
        ):
            try:
                table = pq.read_table(os.path.join(parquet_dir, sub_dir, filename))
                dataframe = pd.read_parquet(
                    os.path.join(parquet_dir, sub_dir, filename)
                )
                metadata = table.schema.metadata
                gt_metadata = json.loads(metadata[b"gittables"].decode())
                # pd_metadata = json.loads(metadata[b"pandas"].decode())
                profile = {
                    "table_id": gt_metadata["table_id"],  # not unique
                    "license": gt_metadata["license"],
                    "csv_url": gt_metadata["csv_url"],
                    "number_rows": gt_metadata["number_rows"],
                    "number_columns": gt_metadata["number_columns"],
                    "table_domain": gt_metadata["table_domain"],
                    "dtypes_percentages": gt_metadata["dtypes_percentages"],
                    "features": [
                        {
                            "name": col,
                            "dtype": gt_metadata["dtypes"][col],
                        }
                        for col in gt_metadata["dtypes"]
                    ],
                }

                for col in profile["features"]:
                    if col["name"] in gt_metadata["dbpedia_syntactic_column_types"]:
                        col["dbpedia_syntactic_column_types"] = gt_metadata[
                            "dbpedia_syntactic_column_types"
                        ][col["name"]]
                    if col["name"] in gt_metadata["schema_syntactic_column_types"]:
                        col["schema_syntactic_column_types"] = gt_metadata[
                            "schema_syntactic_column_types"
                        ][col["name"]]
                    if col["name"] in gt_metadata["dbpedia_semantic_column_types"]:
                        col["dbpedia_semantic_column_types"] = gt_metadata[
                            "dbpedia_semantic_column_types"
                        ][col["name"]]
                    if col["name"] in gt_metadata["dbpedia_semantic_similarities"]:
                        col["dbpedia_semantic_similarities"] = gt_metadata[
                            "dbpedia_semantic_similarities"
                        ][col["name"]]
                    if col["name"] in gt_metadata["schema_semantic_column_types"]:
                        col["schema_semantic_column_types"] = gt_metadata[
                            "schema_semantic_column_types"
                        ][col["name"]]
                    if col["name"] in gt_metadata["schema_semantic_similarities"]:
                        col["schema_semantic_similarities"] = gt_metadata[
                            "schema_semantic_similarities"
                        ][col["name"]]

                    for a in additional_data:
                        try:
                            if a[1](gt_metadata, col["name"]):
                                result = a[2](dataframe[col["name"]])
                                col[a[0]] = result
                        except Exception as e:
                            additional_fails.append(e)
                with open(
                    os.path.join(json_dir, f"{sub_dir}_{filename[:-8]}.json"), "w"
                ) as file:
                    json.dump(profile, file)
            except Exception as e:
                json_fails.append({filename: e})
    if len(json_fails) != 0:
        print(
            f"Failed to create json for the following files due to the respective errors: {json_fails}"
        )
        print(
            f"Failed to create json {len(json_fails)} times. A few fails is the expected behavior."
        )
    if len(additional_fails) != 0:
        print(
            f"Failed to create additional data {len(additional_fails)} times. Some fails is the expected behavior."
        )


if __name__ == "__main__":
    base_path = os.getenv("RAW_DATADIR", "../data")
    base_path = os.path.join(base_path, "gittables")
    record_id = 6517052
    download_and_unzip(record_id, base_path)

    # syntax: (name: str, Callable(metadata: dict, column_name: str), Callable(column: pd.Series)
    numeric = ["int64", "float64"]
    additional = [
        ("null_count", (lambda m, cn: True), (lambda c: int(c.isnull().sum()))),
        (
            "max",
            (lambda m, cn: m["dtypes"][cn] in numeric),
            (lambda c: c.max()),
        ),
        (
            "min",
            (lambda m, cn: m["dtypes"][cn] in numeric),
            (lambda c: c.min()),
        ),
        (
            "mean",
            (lambda m, cn: m["dtypes"][cn] in numeric),
            (lambda c: float(c.mean())),
        ),
        (
            "median",
            (lambda m, cn: m["dtypes"][cn] in numeric),
            (lambda c: c.median()),
        ),
        (
            "stdev",
            (lambda m, cn: m["dtypes"][cn] in numeric),
            (lambda c: c.std()),
        ),
    ]
    create_profiles(base_path, additional)
