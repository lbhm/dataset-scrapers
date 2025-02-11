import argparse
import json
from pathlib import Path

import matplotlib.pyplot as plt


class MetadataAnalyzer:
    def __init__(self, source_dir: Path, output_dir: Path) -> None:
        self.analyzed = 0
        self.record_count = 0
        self.metadata_total_size_wrecordset = 0.0
        self.file_types_count: dict[str, int] = {}
        self.file_sizes: list[float] = []
        self.column_count: list[int] = []
        self.csv_file_count: list[int] = []
        self.unit_multipliers = {
            "B": 1 / (1024),
            "KB": 1,
            "MB": 1024,
            "GB": 1024**2,
            "TB": 1024**3,
        }
        self.source_dir = source_dir
        self.output_dir = output_dir

    def add_count(self, dictionary: dict[str, int], value: str) -> None:
        if value in dictionary:
            dictionary[value] += 1
        else:
            dictionary[value] = 1

    def convert_to_kb(self, string: str) -> float:
        parts = string.split()
        if len(parts) != 2:
            print(f"unexpected string format occurred: {string}")
            quit()
        return float(parts[0]) * self.unit_multipliers[parts[1]]

    def convert_to_highest(self, value: float) -> tuple[float, str]:
        for name, multiplier in sorted(
            self.unit_multipliers.items(), key=lambda item: item[1], reverse=True
        ):
            if value > multiplier:
                return value / multiplier, name
        return value / self.unit_multipliers["B"], "B"

    def analyze_metadata(self, path: Path) -> None:
        # get metadata file from path
        with path.open("r", encoding="utf-8") as file:
            data = json.load(file)
        self.analyzed += 1

        # analyze distribution
        distribution = data["distribution"]
        nfiles = 0
        size = "0 B"
        for item in distribution:
            # csv in @id indicates a tabular file
            if "csv" in item["@id"]:
                nfiles += 1
            # contentSize indicates size of .zip download file
            if "contentSize" in item:
                size = item["contentSize"]

        self.csv_file_count.append(nfiles)
        # no csv files so we continue
        if nfiles == 0:
            return

        self.file_sizes.append(self.convert_to_kb(size))

        # analyze recordSet if given
        if "recordSet" not in data:
            return

        self.metadata_total_size_wrecordset += path.stat().st_size / 1024

        records = data["recordSet"]
        self.record_count += 1
        for file_record in records:
            columns = 0
            # collect data types in columns
            for column in file_record["field"]:
                data_type = column["dataType"][0].rsplit(":", 1)[-1]
                self.add_count(self.file_types_count, data_type)
                columns += 1
            # collect column counts
            self.column_count.append(columns)

    def plot_csv_file_count(self, max_files: int = 100) -> None:
        total = len(self.csv_file_count)
        tabular = len([count for count in self.csv_file_count if count > 0])
        csv_file_count = [count for count in self.csv_file_count if count < max_files]
        plt.figure(f"CSV file count until {max_files} files")
        plt.hist(csv_file_count, bins=500, color="blue", edgecolor="black", alpha=0.7)
        plt.xlabel("CSV files in a dataset")
        plt.ylabel("frequency")
        plt.title(f"CSV file count distribution ({round(tabular / total * 100, 2)}% > 0)")
        plt.savefig(self.output_dir / "csv_file_count.png")

    def plot_file_sizes(self, max_size: int = 100000) -> None:
        sum_size, unit = self.convert_to_highest(sum(self.file_sizes))
        # use filter to remove outliers
        file_sizes = [count for count in self.file_sizes if count < max_size]
        filter_sum_size, filter_unit = self.convert_to_highest(sum(file_sizes))
        filter_len = len(file_sizes)

        max_size_highest, max_size_unit_highest = self.convert_to_highest(max_size)
        plt.figure(f"sizes of tabular datasets until {max_size} KB")
        plt.hist(file_sizes, bins=500, color="red", edgecolor="black", alpha=0.7)
        plt.xlabel("dataset sizes (KB)")
        plt.ylabel("frequency")
        plt.title(
            f"dataset size distribution of tabular datasets (.zip file). Total: {round(sum_size, 2)} {unit}"
        )
        plt.savefig(self.output_dir / "file_sizes.png")
        print(
            f"Total size: {round(sum_size, 2)} {unit}. Filtered size (<{round(max_size_highest, 2)} {max_size_unit_highest}): {round(filter_sum_size, 2)} {filter_unit} ({filter_len} datasets)"
        )

    def plot_column_count(self, max_columns: int = 100) -> None:
        column_count = [count for count in self.column_count if count < max_columns]
        plt.figure(f"column counts of tabular datasets until {max_columns} columns")
        plt.hist(column_count, bins=500, color="yellow", edgecolor="black", alpha=0.7)
        plt.xlabel("columns of tabular files")
        plt.ylabel("frequency")
        plt.title("column count distribution of datasets with recordset key")
        plt.savefig(self.output_dir / "column_count.png")

    def plot_file_types(self) -> None:
        plt.figure("file types of tabular datasets")
        x, y = list(self.file_types_count.keys()), list(self.file_types_count.values())
        plt.bar(x, y, width=0.8, color="green", edgecolor="black")
        plt.grid(axis="y")
        plt.xlabel("file types of tabular files")
        plt.ylabel("frequency")
        plt.title(
            f"file type distribution of datasets with recordset key ({round(self.record_count / self.analyzed * 100, 2)}%)"
        )
        plt.savefig(self.output_dir / "file_types.png")
        print(
            f"Analyzed {self.analyzed} datasets. {self.record_count} datasets with recordset key."
        )
        total_columns = 0
        numeric_columns = 0
        for data_type, count in self.file_types_count.items():
            total_columns += count
            if data_type == "Integer" or data_type == "Float":
                numeric_columns += count
        print(f"Total columns: {total_columns}. Numeric columns: {numeric_columns}")

    def print_metadata_size(self) -> None:
        value, unit = self.convert_to_highest(self.metadata_total_size_wrecordset)
        print(f"Total size of metadata with recordset key: {round(value, 2)} {unit}")

    def start(self) -> None:
        for path in self.source_dir.rglob("croissant_metadata.json"):
            try:
                self.analyze_metadata(path)
            except Exception as e:
                print(f"Error occurred with {path}: {e}")


def main() -> None:
    parser = argparse.ArgumentParser(description="analyze kaggle metadata")
    parser.add_argument(
        "--source", type=str, help="path to metadata", default="../kaggle_metadata"
    )
    parser.add_argument("--output", type=str, help="output path for plots", default="../plots")
    args = parser.parse_args()
    kaggle_path = Path(args.source)
    output_dir = Path(args.output)
    output_dir.mkdir(exist_ok=True, parents=True)

    if not kaggle_path.exists():
        print("This program requires a directory with croissant metadata to work!")
        return

    analyzer = MetadataAnalyzer(kaggle_path, output_dir)
    analyzer.start()

    analyzer.plot_csv_file_count()
    analyzer.plot_file_sizes()
    analyzer.plot_column_count()
    analyzer.plot_file_types()

    analyzer.print_metadata_size()

    plt.show()


if __name__ == "__main__":
    main()
