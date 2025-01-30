import os
import json
import pandas as pd
import numpy as np
import cchardet
import tqdm
from collections import Counter
from pathlib import Path
import argparse

os.chdir(os.path.dirname(os.path.abspath(__file__)))

RESULT_DIR = Path("../croissant")
SOURCE_DIR = None

error_count = 0

def detect_separator(csv_file: str, encoding: str) -> str:
    possible_separators = [",", ";", "\t", "|"]
    with open(csv_file, "r", encoding=encoding) as f:
        sample = f.readline()
        counts = Counter({sep: sample.count(sep) for sep in possible_separators})
    return max(counts, key=counts.get) if counts else ","

def calculate_completeness(metadata) -> float:
    score = 0
    max_score = 5
    if metadata["license"]["name"] != "Unknown":
        score += 1
    if metadata["alternateName"] != "":
        score += 1
    if metadata["description"] != "":
        score += 1
    for file in metadata["distribution"]:
        if "contentSize" not in file and "description" in file:
            score += 1
            break
    for record in metadata["recordSet"]:
        for column in record["field"]:
            if "description" in column:
                score += 1
                break
    return score / max_score
    

def process_dataset(path: Path, bin_count: int):
    # get metadata
    metadata_path = path / "croissant_metadata.json"
    with open(metadata_path, "r", encoding="utf-8") as file:
        metadata = json.load(file)   
    records = metadata["recordSet"]

    score = calculate_completeness(metadata)
    metadata["usability"] = score
    
    for file in records:
        csv_file = path / file["@id"].replace("+", " ").replace("/", "_")
        # guess appropiate encoding
        with open(csv_file, "rb") as f:
            result = cchardet.detect(f.read())
            encoding = result["encoding"]
        # try to find correct separator
        delimiter = detect_separator(csv_file, encoding)
        df = pd.read_csv(csv_file, encoding=encoding, sep=delimiter, engine="python", on_bad_lines="skip")
        # remove unnecessary spaces
        df.columns = df.columns.str.strip()
        for n, column in enumerate(file["field"]):
            data_type = column["dataType"][0].rsplit(":", 1)[-1]
            column_name = column["name"]
            # catch case where column has empty name
            if column_name == "":
                column_name = f"Unnamed: {n}"
            data = list(df[column_name].dropna())
            # case for numeric columns
            if data_type == "Integer" or data_type == "Float":
                if isinstance(data[0], str):
                    try:
                        # catch case where 1923423 = "1,923,423"
                        data = [float(d.replace(",", "")) for d in data]
                    except:
                        # map strings to numbers
                        unique_strings = list(set(data))
                        mapping = {string: idx for idx, string in enumerate(unique_strings)}
                        data = [mapping[item] for item in data]
                # create histogram
                densities, bins = np.histogram(data, density=True, bins=bin_count)
                # save hist
                column["histogram"] = {"bins": list(bins), "densities": list(densities / np.sum(densities))}
                column["statistics"] = df[column_name].dropna().describe().to_dict()
            # case for text columns
            elif data_type == "Text":
                n_unique = len(set(data))
                top_10 = dict(Counter(data).most_common(10))
                column["n_unique"] = n_unique
                column["most_common"] = top_10
            elif data_type == "Boolean":
                if isinstance(data[0], str):
                    data = [d.lower() for d in data]
                    if data[0] == "true" or data[0] == "false":
                        data = [d == "true" for d in data]
                    elif data[0] == "yes" or data[0] == "no":
                        data = [d == "yes" for d in data]
                    elif data[0] == "t" or data[0] == "f":
                        data = [d == "t" for d in data]
                elif isinstance(data[0], float) or isinstance(data[0], int):
                    max_value = max(data)
                    data = [d == max_value for d in data]
                n_true = sum(data)
                n_false = len(data) - n_true
                column["n_true"] = n_true
                column["n_false"] = n_false

    # write metadata in RESULT_DIR
    ref = "/".join(str(path).split("/")[-2:]).replace("/", "_")
    with open(RESULT_DIR / (ref + ".json"), "w") as f:
        json.dump(metadata, f, indent=4, ensure_ascii=False)

def create_histograms(max_datasets: int, bin_count: int = 10):
    global error_count
    process_list : list[Path] = []
    for path in SOURCE_DIR.rglob("croissant_metadata.json"):
        if len(list(path.parent.iterdir())) > 1:
            process_list.append(path.parent)
    process_list = process_list[:min(max_datasets, len(process_list))]
    with tqdm.tqdm(total=len(process_list), desc="processing datasets") as progress:
        for dataset_path in process_list:
            try:
                process_dataset(dataset_path, bin_count)
            except Exception as e:
                print(f"Error occurred with {dataset_path}: {e}")
                error_count += 1
            progress.update(1)
    print(f"{len(process_list) - error_count} of {len(process_list)} datasets processed ({round((len(process_list) - error_count)/len(process_list) * 100)}%)")
        
def main():
    global SOURCE_DIR
    parser = argparse.ArgumentParser(description="create histograms for kaggle datasets")
    parser.add_argument("--path", type=str, help="path to metadata", default="../kaggle_metadata")
    parser.add_argument("--count", type=int, help="max count of datasets to be processed", default=10**1000)
    args = parser.parse_args()
    SOURCE_DIR = Path(args.path)
    max_count = args.count
    RESULT_DIR.mkdir(exist_ok=True)
    create_histograms(max_count)
    print("Done!")

if __name__ == "__main__":
    main()
    