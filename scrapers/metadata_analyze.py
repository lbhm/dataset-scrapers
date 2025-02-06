from itertools import count
from pathlib import Path
import json
import matplotlib.pyplot as plt
import os
import argparse

os.chdir(os.path.dirname(os.path.abspath(__file__)))

OUTPUT_DIR = Path("../plots")
OUTPUT_DIR.mkdir(exist_ok=True, parents=True)

analyzed = 0 # datasets analyzed in total
record_count = 0 # datasets with recordset
metadata_total_size_wrecordset = 0

file_types_count = {} # file type : frequency
file_sizes = []
column_count = []
csv_file_count = []

unit_multipliers = {
        "B": 1/(1024),
        "KB": 1,
        "MB": 1024,
        "GB": 1024**2,
        "TB": 1024**3
    }

def add_count(dictionary: dict, value: int):
    if value in dictionary:
        dictionary[value] += 1
    else:
        dictionary[value] = 1
    
def convert_to_kb(string: str) -> float:
    parts = string.split()
    if len(parts) != 2:
        print(f"unexpected string format occurred: {string}")
        quit()
    return float(parts[0]) * unit_multipliers[parts[1]]

def convert_to_highest(value: float) -> tuple[float, str]:
    for name, multiplier in sorted(unit_multipliers.items(), key=lambda item: item[1], reverse=True):
        if value > multiplier:
            return value / multiplier, name

def analyze_metadata(path: Path):
    global analyzed, record_count, metadata_total_size_wrecordset
    # get metadata file from path
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    analyzed += 1
    
    # analyze distribution
    distribution = data["distribution"]
    nfiles = 0
    for item in distribution:
        # csv in @id indicates a tabular file
        if "csv" in item["@id"]:
            nfiles += 1
        # contentSize indicates size of .zip download file
        if "contentSize" in item:
            size = item["contentSize"]
    
    csv_file_count.append(nfiles)
    # no csv files so we continue
    if nfiles == 0:
        return

    file_sizes.append(convert_to_kb(size))

    # analyze recordSet if given
    if "recordSet" not in data:
        return
    
    metadata_total_size_wrecordset += os.path.getsize(path) / 1024

    records = data["recordSet"]
    record_count += 1
    for file in records:
        columns = 0
        # collect data types in columns
        for column in file["field"]:
            data_type = column["dataType"][0].rsplit(":", 1)[-1]
            add_count(file_types_count, data_type)
            columns += 1
        # collect column counts
        column_count.append(columns)
        
def plot_csv_file_count(max_files = 100):
    global csv_file_count
    total = len(csv_file_count)
    tabular = len([count for count in csv_file_count if count > 0])
    csv_file_count = [count for count in csv_file_count if count < max_files]
    plt.figure(f"CSV file count until {max_files} files")
    plt.hist(csv_file_count, bins=500, color='blue', edgecolor='black', alpha=0.7)
    plt.xlabel("CSV files in a dataset")
    plt.ylabel("frequency")
    plt.title(f"CSV file count distribution ({round(tabular/total * 100, 2)}% > 0)")
    plt.savefig(OUTPUT_DIR / "csv_file_count.png") 
    
def plot_file_sizes(max_size = 100000):
    global file_sizes
    sum_size, unit = convert_to_highest(sum(file_sizes))
    # use filter to remove outliers
    file_sizes = [count for count in file_sizes if count < max_size]
    filter_sum_size, filter_unit = convert_to_highest(sum(file_sizes))
    filter_len = len(file_sizes)

    max_size_highest, max_size_unit_highest = convert_to_highest(max_size)
    plt.figure(f"sizes of tabular datasets until {max_size} KB")
    plt.hist(file_sizes, bins=500, color='red', edgecolor='black', alpha=0.7)
    plt.xlabel("dataset sizes (KB)")
    plt.ylabel("frequency")
    plt.title(f"dataset size distribution of tabular datasets (.zip file). Total: {round(sum_size, 2)} {unit}")
    plt.savefig(OUTPUT_DIR / "file_sizes.png")
    print(f"Total size: {round(sum_size, 2)} {unit}. Filtered size (<{round(max_size_highest, 2)} {max_size_unit_highest}): {round(filter_sum_size, 2)} {filter_unit} ({filter_len} datasets)")
    
def plot_column_count(max_columns = 100):
    global column_count
    column_count = [count for count in column_count if count < max_columns]
    plt.figure(f"column counts of tabular datasets until {max_columns} columns")
    plt.hist(column_count, bins=500, color='yellow', edgecolor='black', alpha=0.7)
    plt.xlabel("columns of tabular files")
    plt.ylabel("frequency")
    plt.title(f"column count distribution of datasets with recordset key") 
    plt.savefig(OUTPUT_DIR / "column_count.png")
    
def plot_file_types():
    plt.figure("file types of tabular datasets")
    x, y = list(file_types_count.keys()), list(file_types_count.values())
    plt.bar(x, y, width = 0.8, color="green", edgecolor="black")
    plt.grid(axis="y")
    plt.xlabel("file types of tabular files")
    plt.ylabel("frequency")
    plt.title(f"file type distribution of datasets with recordset key ({round(record_count / analyzed * 100, 2)}%)")
    plt.savefig(OUTPUT_DIR / "file_types.png")
    print(f"Analyzed {analyzed} datasets. {record_count} datasets with recordset key.")
    total_columns = 0
    numeric_columns = 0
    for type, count in file_types_count.items():
        total_columns += count
        if type == "Integer" or type == "Float":
            numeric_columns += count
    print(f"Total columns: {total_columns}. Numeric columns: {numeric_columns}")


def main():
    parser = argparse.ArgumentParser(description="analyze kaggle metadata")
    parser.add_argument("--path", type=str, help="path to metadata", default="../kaggle_metadata")
    args = parser.parse_args()
    kaggle_path = args.path

    if os.path.exists(kaggle_path):
        METADATA_DIR = Path(kaggle_path)
    else:
        print(f"Warning: Path does not exist. Using ../kaggle_metadata instead...")
        METADATA_DIR = Path("../kaggle_metadata")

    for path in METADATA_DIR.rglob("croissant_metadata.json"):
            try:
                analyze_metadata(path)
            except Exception as e:
                print(f"Error occurred with {path}: {e}") 
    
    plot_csv_file_count()
    plot_file_sizes()
    plot_column_count()
    plot_file_types()
    value, unit = convert_to_highest(metadata_total_size_wrecordset)
    print(f"Total size of metadata with recordset key: {round(value, 2)} {unit}")

    plt.show()
    
if __name__ == "__main__":
    main()