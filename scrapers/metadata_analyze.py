from operator import mul
from pathlib import Path
import json
import matplotlib.pyplot as plt
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

METADATA_DIR = "../kaggle_metadata" 

analyzed = 0 # datasets analyzed in total
record_count = 0 # datasets with recordset

file_types_count = {} # file type : frequency
file_sizes = []
row_count = []
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
    global analyzed, record_count
    # get metadata file from path
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    analyzed += 1
    
    # analyze distribution
    distribution = data["jsonld"]["distribution"]
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
    if "recordSet" not in data["jsonld"]:
        return
    records = data["jsonld"]["recordSet"]
    record_count += 1
    for file in records:
        rows = 0
        # collect data types in rows
        for row in file["field"]:
            data_type = row["dataType"][0].rsplit(":", 1)[-1]
            add_count(file_types_count, data_type)
            rows += 1
        # collect row counts
        row_count.append(rows)
        
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
    
def plot_file_sizes(max_size = 2000):
    global file_sizes
    sum_size, unit = convert_to_highest(sum(file_sizes))
    file_sizes = [count for count in file_sizes if count < max_size]
    plt.figure(f"sizes of tabular datasets until {max_size} KB")
    plt.hist(file_sizes, bins=500, color='red', edgecolor='black', alpha=0.7)
    plt.xlabel("dataset sizes (KB)")
    plt.ylabel("frequency")
    plt.title(f"dataset size distribution of tabular datasets (.zip file). Total: {round(sum_size, 2)} {unit}")
    
def plot_row_count(max_rows = 100):
    global row_count
    row_count = [count for count in row_count if count < max_rows]
    plt.figure(f"row counts of tabular datasets until {max_rows} rows")
    plt.hist(row_count, bins=500, color='yellow', edgecolor='black', alpha=0.7)
    plt.xlabel("rows of tabular files")
    plt.ylabel("frequency")
    plt.title(f"row count distribution of datasets with recordset key")   
    
def plot_file_types():
    plt.figure("file types of tabular datasets")
    x, y = list(file_types_count.keys()), list(file_types_count.values())
    plt.bar(x, y, width = 0.8, color="green", edgecolor="black")
    plt.grid(axis="y")
    plt.xlabel("file types of tabular files")
    plt.ylabel("frequency")
    plt.title(f"file type distribution of datasets with recordset key ({round(record_count / analyzed * 100, 2)}%)")   

def main():
    for path in Path(METADATA_DIR).rglob("*"):
        if path.is_file():
            analyze_metadata(path)
    
    plot_csv_file_count()
    plot_file_sizes()
    plot_row_count()
    plot_file_types()

    plt.show()
    
if __name__ == "__main__":
    main()