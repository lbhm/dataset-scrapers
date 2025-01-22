import os
import json
import pandas as pd
import numpy as np
import cchardet
import tqdm
from collections import Counter

os.chdir(os.path.dirname(os.path.abspath(__file__)))

SOURCE_DIR = "../kaggle_metadata"
RESULT_DIR = "../croissant"
total_count = 0

def detect_separator(csv_file: str, encoding: str) -> str:
    possible_separators = [",", ";", "\t", "|"]
    with open(csv_file, "r", encoding=encoding) as f:
        sample = f.readline()
        counts = Counter({sep: sample.count(sep) for sep in possible_separators})
    return max(counts, key=counts.get) if counts else ","
        

def process_dataset(path: str, bin_count: int):
    # check if dataset is empty
    if len(os.listdir(path)) <= 1:
        return
    # get metadata
    metadata_path = os.path.join(path, "metadata.json")
    with open(metadata_path, "r", encoding="utf-8") as file:
        metadata = json.load(file)   
    records = metadata["recordSet"]
    
    for file in records:
        csv_file = os.path.join(path, file["@id"].replace("+", " ").replace("/", "_"))
        # guess appropiate encoding
        with open(csv_file, "rb") as f:
            result = cchardet.detect(f.read())
            encoding = result["encoding"]
        # try to find correct separator
        delimiter = detect_separator(csv_file, encoding)
        df = pd.read_csv(csv_file, encoding=encoding, sep=delimiter, low_memory=False)
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

    # write metadata in RESULT_DIR
    ref = "/".join(path.split("/")[2:]).replace("/", "_")
    with open(os.path.join(RESULT_DIR, ref + ".json"), "w") as f:
        json.dump(metadata, f, indent=4, ensure_ascii=False)

def count_total():
    global total_count
    for user_folder in os.listdir(SOURCE_DIR):
        user_path = os.path.join(SOURCE_DIR, user_folder)
        if os.path.isdir(user_path):
            for dataset_folder in os.listdir(user_path):
                dataset_path = os.path.join(user_path, dataset_folder)
                if os.path.isdir(dataset_path):
                    total_count += 1                   
                        
def create_histograms(bin_count: int = 10):
    with tqdm.tqdm(total=total_count, desc="processing datasets") as progress:
        for user_folder in os.listdir(SOURCE_DIR):
            user_path = os.path.join(SOURCE_DIR, user_folder)
            if os.path.isdir(user_path):
                for dataset_folder in os.listdir(user_path):
                    dataset_path = os.path.join(user_path, dataset_folder)
                    if os.path.isdir(dataset_path):
                        try:
                            process_dataset(dataset_path, bin_count)
                        except Exception as e:
                            print(f"Error occurred with {dataset_path}: {e}")
                        progress.update(1)

def main():
    os.makedirs(RESULT_DIR, exist_ok=True)
    count_total()
    create_histograms()
    print("Done!")

if __name__ == "__main__":
    main()
    