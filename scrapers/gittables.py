import io
import json
import os
import zipfile

import numpy as np
import requests
from pyarrow import parquet as pq
from tqdm import tqdm


def download(r_id: int, path: str):
    response = requests.get(f"https://zenodo.org/api/records/{r_id}")
    if response.status_code == 200:
        record_info = response.json()
        files = record_info["files"]
        fails = []
        for file_info in tqdm(files):
            download_url = file_info["links"]["self"]
            response = requests.get(download_url)
            if response.status_code == 200:
                with open(
                    os.path.join(path, "zip_files", file_info["key"]), "wb"
                ) as file:
                    file.write(response.content)
            else:
                fails.append(file_info["key"])
        if len(fails) != 0:
            print(f"Failed to download files: {fails}")
    else:
        print(f"HTTP request failed. response code: {response.status_code}")


def unzip(path: str):
    zip_dir = os.path.join(path, "zip_files")
    for filename in tqdm(os.listdir(zip_dir)):
        file_path = os.path.join(zip_dir, filename)
        if zipfile.is_zipfile(file_path):
            with zipfile.ZipFile(file_path, "r") as zip_file:
                zip_file.extractall(os.path.join(path, "parquets", filename))


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
    parquet_dir = os.path.join(path, "test")
    json_dir = os.path.join(path, "jsons")
    os.makedirs(json_dir, exist_ok=True)
    fails = []
    c = 0
    datatypes = set()
    for sub_dir in os.listdir(parquet_dir):
        for filename in tqdm(os.listdir(os.path.join(parquet_dir, sub_dir))):
            try:
                parquet = pq.read_table(os.path.join(parquet_dir, sub_dir, filename))
                metadata = parquet.schema.metadata
                gt_metadata = json.loads(metadata[b"gittables"].decode())
                pd_metadata = json.loads(metadata[b"pandas"].decode())
                profile = {
                    "table_id": gt_metadata["table_id"],  # not unique
                    "license": gt_metadata["license"],
                    "csv_url": gt_metadata["license"],
                    "number_rows": gt_metadata["number_rows"],
                    "number_columns": gt_metadata["number_columns"],
                    "table_domain": gt_metadata["table_domain"],
                    "dtypes_percentages": gt_metadata["dtypes_percentages"],
                    "features": [
                        {
                            "name": col,
                            "dtype": gt_metadata["dtypes"][col]
                            if col in gt_metadata["dtypes"]
                            else None,
                            "dbpedia_syntactic_column_type": gt_metadata[
                                "dbpedia_syntactic_column_types"
                            ][col]
                            if col in gt_metadata["dbpedia_syntactic_column_types"]
                            else None,
                            "schema_syntactic_column_type": gt_metadata[
                                "schema_syntactic_column_types"
                            ][col]
                            if col in gt_metadata["schema_syntactic_column_types"]
                            else None,
                            "dbpedia_semantic_column_type": gt_metadata[
                                "dbpedia_semantic_column_types"
                            ][col]
                            if col in gt_metadata["dbpedia_semantic_column_types"]
                            else None,
                            "dbpedia_semantic_similarity": gt_metadata[
                                "dbpedia_semantic_similarities"
                            ][col]
                            if col in gt_metadata["dbpedia_semantic_similarities"]
                            else None,
                            "schema_semantic_column_type": gt_metadata[
                                "schema_semantic_column_types"
                            ][col]
                            if col in gt_metadata["schema_semantic_column_types"]
                            else None,
                            "schema_semantic_similarity": gt_metadata[
                                "schema_semantic_similarities"
                            ][col]
                            if col in gt_metadata["schema_semantic_similarities"]
                            else None,
                        }
                        for col in gt_metadata["dtypes"]
                    ],
                }

                for col in profile["features"]:
                    datatypes.add(gt_metadata["dtypes"][col["name"]])
                    for a in additional_data:
                        if a[1](gt_metadata, col["name"]):
                            result = a[2](parquet.column(col["name"]))
                            if not np.isnan(result):
                                col[a[0]] = result
                            else:
                                col[a[0]] = None
                            c += 1
                with open(
                    os.path.join(json_dir, f"{sub_dir}_{filename[:-8]}.json"), "w"
                ) as file:
                    json.dump(profile, file)
            except Exception as e:
                fails.append({filename: e})
    if len(fails) != 0:
        print(
            f"Failed to create json for the following files due to the respective errors: {fails}"
        )
        print(len(fails))
    print(f"data types: {datatypes}")
    print(f"additional metadata created: {c}")


if __name__ == "__main__":
    base_path = os.getenv("RAW_DATADIR", "../data")
    base_path = os.path.join(base_path, "gittables")
    record_id = 6517052
    # download_and_unzip(record_id, base_path)

    # syntax: ('name', condition(metadata, column_name), function(column))
    numeric = ["int64", "float64"]
    additional = [
        ("null_count", (lambda g, c: True), (lambda c: c.null_count)),
        (
            "max",
            (lambda g, c: g["dtypes"][c] in numeric),
            (lambda c: np.max(c.to_pandas())),
        ),
        (
            "min",
            (lambda g, c: g["dtypes"][c] in numeric),
            (lambda c: np.min(c.to_pandas())),
        ),
        (
            "average",
            (lambda g, c: g["dtypes"][c] in numeric),
            (lambda c: np.average(c.to_pandas())),
        ),
        (
            "median",
            (lambda g, c: g["dtypes"][c] in numeric),
            (lambda c: np.median(c.to_pandas())),
        ),
        (
            "stdev",
            (lambda g, c: g["dtypes"][c] in numeric),
            (lambda c: np.std(c.to_pandas())),
        ),
    ]
    create_profiles(base_path, additional)
