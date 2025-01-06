from pathlib import Path
import json
import matplotlib.pyplot as plt
import os

os.chdir(os.path.dirname(os.path.abspath(__file__)))

METADATA_DIR = "../kaggle_metadata" 

analyzed = 0

file_types_count = {} # file type : frequency
file_sizes = []
row_count = []
csv_file_count = []

def add_count(dictionary: dict, value: int):
    if value in dictionary:
        dictionary[value] += 1
    else:
        dictionary[value] = 1
    
def convert_to_kb(string: str) -> float:
    unit_multipliers = {
        "B": 1/(1024),
        "KB": 1,
        "MB": 1024,
        "GB": 1024**2,
        "TB": 1024**3
    }
    parts = string.split()
    if len(parts) != 2:
        print(f"unexpected string format occurred: {string}")
        quit()
    return float(parts[0]) * unit_multipliers[parts[1]]

def analyze_metadata(path: Path):
    global analyzed
    with path.open("r", encoding="utf-8") as file:
        data = json.load(file)
    analyzed += 1
    
    distribution = data["jsonld"]["distribution"]
    nfiles = 0
    for item in distribution:
        if "csv" in item["@id"]:
            nfiles += 1
        if "contentSize" in item:
            size = item["contentSize"]
            
    # no csv files so we ignore
    if nfiles == 0:
        return
    csv_file_count.append(nfiles)
    file_sizes.append(convert_to_kb(size))
    
    if "recordSet" not in data["jsonld"]:
        return
    records = data["jsonld"]["recordSet"]
    for file in records:
        rows = 0
        for row in file["field"]:
            data_type = row["dataType"][0].rsplit(":", 1)[-1]
            add_count(file_types_count, data_type)
            rows += 1
        row_count.append(rows)
    


def main():
    for path in Path(METADATA_DIR).rglob("*"):
        if path.is_file():
            analyze_metadata(path)
    global csv_file_count, file_sizes, row_count
    csv_file_count = [count for count in csv_file_count if count < 100]
    plt.figure("CSV file count")
    plt.hist(csv_file_count, bins=500, color='blue', edgecolor='black', alpha=0.7)
    plt.xlabel("CSV files in a dataset")
    plt.ylabel("frequency")
    plt.title("CSV file count distribution")
    
    file_sizes = [count for count in file_sizes if count < 2000]
    plt.figure("total sizes of tabular datasets")
    plt.hist(file_sizes, bins=500, color='red', edgecolor='black', alpha=0.7)
    plt.xlabel("file sizes (KB)")
    plt.ylabel("frequency")
    plt.title("file size distribution of tabular datasets")
    
    row_count = [count for count in row_count if count < 100]
    plt.figure("row counts")
    plt.hist(row_count, bins=500, color='yellow', edgecolor='black', alpha=0.7)
    plt.xlabel("rows of tabular files")
    plt.ylabel("frequency")
    plt.title("row count distribution of tabular files in datasets")
    
    plt.figure("file types")
    x, y = list(file_types_count.keys()), list(file_types_count.values())
    plt.bar(x, y, width = 0.8, color="green", edgecolor="black")
    plt.grid(axis="y")
    plt.xlabel("file types of tabular files")
    plt.ylabel("frequency")
    plt.title("file type distribution of tabular files in datasets")
    
    plt.show()
    
            
            


if __name__ == "__main__":
    main()