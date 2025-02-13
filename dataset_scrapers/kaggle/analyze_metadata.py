import argparse
import json
import sys
from collections import defaultdict
from pathlib import Path

import matplotlib.pyplot as plt


class MetadataAnalyzer:
    def __init__(self, source_dir: Path, output_dir: Path) -> None:
        self.source_dir = source_dir
        self.output_dir = output_dir
        self.analyzed = 0
        self.record_count = 0
        self.metadata_total_size_wrecordset = 0.0
        self.data_type_count: dict[str, int] = defaultdict(int)
        self.file_sizes: list[float] = []
        self.column_count: list[int] = []
        self.csv_file_count: list[int] = []

        # NOTE: This could be moved to a utils file
        self.unit_multipliers: dict[str, float] = {
            "B": 1 / (1024),
            "KB": 1,
            "MB": 1024,
            "GB": 1024**2,
            "TB": 1024**3,
        }

    def convert_to_kb(self, file_size: str) -> float:
        parts = file_size.split()
        if len(parts) != 2 or parts[1] not in self.unit_multipliers:
            raise ValueError(f"Unexpected file size format occurred: {file_size}")

        return float(parts[0]) * self.unit_multipliers[parts[1]]

    def convert_kb_to_highest_prefix(self, value: float) -> tuple[float, str]:
        for prefix, multiplier in sorted(
            self.unit_multipliers.items(), key=lambda item: item[1], reverse=True
        ):
            if value > multiplier:
                return value / multiplier, prefix
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
                self.data_type_count[data_type] += 1
                columns += 1
            # collect column counts
            self.column_count.append(columns)

    def plot_csv_file_count(self, max_files: int = 100) -> None:
        total = len(self.csv_file_count)
        tabular = len([count for count in self.csv_file_count if count > 0])
        csv_file_count = [count for count in self.csv_file_count if count < max_files]
        plt.figure()
        plt.hist(csv_file_count, bins=500, color="blue", edgecolor="black", alpha=0.7)
        plt.xlabel("Number of CSV files in a dataset")
        plt.ylabel("Frequency")
        plt.title(f"CSV file count distribution ({round(tabular / total * 100, 2)}% > 0)")
        plt.savefig(self.output_dir / "csv_file_count.png")

    def plot_file_sizes(self, max_size: int = 100000) -> None:
        sum_size, unit = self.convert_kb_to_highest_prefix(sum(self.file_sizes))
        # use filter to remove outliers
        file_sizes = [count for count in self.file_sizes if count < max_size]
        filter_sum_size, filter_unit = self.convert_kb_to_highest_prefix(sum(file_sizes))
        filter_len = len(file_sizes)

        max_size_highest, max_size_unit_highest = self.convert_kb_to_highest_prefix(max_size)
        plt.figure(f"sizes of tabular datasets until {max_size} KB")
        plt.hist(file_sizes, bins=500, color="red", edgecolor="black", alpha=0.7)
        plt.xlabel("Dataset sizes (KB)")
        plt.ylabel("Frequency")
        plt.title(
            "Dataset size distribution of tabular datasets (.zip files), "
            f"Total: {round(sum_size, 2)} {unit}"
        )
        plt.savefig(self.output_dir / "file_sizes.png")

        print(
            f"Total size: {round(sum_size, 2)} {unit}. "
            f"Filtered size (<{round(max_size_highest, 2)} {max_size_unit_highest}): "
            f"{round(filter_sum_size, 2)} {filter_unit} ({filter_len} datasets)"
        )

    def plot_column_count(self, max_columns: int = 100) -> None:
        column_count = [count for count in self.column_count if count < max_columns]
        plt.figure()
        plt.hist(column_count, bins=500, color="yellow", edgecolor="black", alpha=0.7)
        plt.xlabel("Number of columns")
        plt.ylabel("Frequency")
        plt.title("Column count distribution of datasets with recordset key")
        plt.savefig(self.output_dir / "column_count.png")

    def plot_file_types(self) -> None:
        x, y = list(self.data_type_count.keys()), list(self.data_type_count.values())
        plt.figure()
        plt.bar(x, y, width=0.8, color="green", edgecolor="black")
        plt.grid(axis="y")
        plt.xlabel("File types of tabular files")
        plt.ylabel("Frequency")
        plt.title(
            "File type distribution of datasets with recordset key "
            f"({round(self.record_count / self.analyzed * 100, 2)}%)"
        )
        plt.savefig(self.output_dir / "file_types.png")

        print(
            f"Analyzed {self.analyzed} datasets, {self.record_count} datasets with recordset key."
        )
        total_columns = 0
        numeric_columns = 0
        for data_type, count in self.data_type_count.items():
            total_columns += count
            if data_type.lower() in ["int", "integer", "float"]:
                numeric_columns += count
        print(f"Total columns: {total_columns}, Numeric columns: {numeric_columns}")

    def print_metadata_size(self) -> None:
        value, unit = self.convert_kb_to_highest_prefix(self.metadata_total_size_wrecordset)
        print(f"Total size of metadata with recordset key: {round(value, 2)} {unit}")

    def start(self) -> None:
        for path in self.source_dir.rglob("croissant_metadata.json"):
            try:
                self.analyze_metadata(path)
            except Exception as e:
                print(f"Error occurred with {path}: {e}")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="analyze kaggle metadata")
    parser.add_argument(
        "--source",
        type=str,
        default="../kaggle_metadata",
        help="path to metadata (default %(default)s)",
    )
    parser.add_argument(
        "--output",
        type=str,
        default="../plots",
        help="output path for plots (default %(default)s)",
    )
    parser.add_argument(
        "--max-files",
        type=int,
        default=100,
        help="limit for the file count analysis (default %(default)s)",
    )
    parser.add_argument(
        "--max-size",
        type=int,
        default=100000,
        help="limit for the file size analysis in bytes (default %(default)s)",
    )
    parser.add_argument(
        "--max-columns",
        type=int,
        default=100,
        help="limit for the column count analysis (default %(default)s)",
    )
    parser.add_argument("--show-plots", action="store_true", help="render plots after analysis")
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    kaggle_path = Path(args.source)
    output_dir = Path(args.output)
    output_dir.mkdir(exist_ok=True, parents=True)
    if not kaggle_path.exists():
        print("This program requires a directory with croissant metadata to work!")
        sys.exit(1)

    analyzer = MetadataAnalyzer(kaggle_path, output_dir)
    analyzer.start()

    analyzer.plot_csv_file_count(max_files=args.max_files)
    analyzer.plot_file_sizes(max_size=args.max_size)
    analyzer.plot_column_count(max_columns=args.max_columns)
    analyzer.plot_file_types()
    analyzer.print_metadata_size()

    if args.show_plots:
        plt.show()


if __name__ == "__main__":
    main()
