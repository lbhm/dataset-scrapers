import argparse
import json
import sys
from collections import Counter
from pathlib import Path
from typing import Any

import cchardet
import numpy as np
import pandas as pd
import tqdm


class HistogramCreator:
    def __init__(
        self, source_dir: Path, target_dir: Path, max_count: int, bin_count: int = 10
    ) -> None:
        self.source_dir = source_dir
        self.target_dir = target_dir
        self.max_count = max_count
        self.bin_count = bin_count
        self.error_count = 0

    def analyze_csv_file(self, path: Path) -> tuple[str, str]:
        """Analyze a CSV file and return its encoding and separator."""
        possible_separators = [",", ";", "\t", "|"]

        with path.open("rb") as file:
            result = cchardet.detect(file.read())
            encoding = result["encoding"]

        with path.open("r", encoding=encoding) as file:
            sample = file.readline()
            counts = Counter({sep: sample.count(sep) for sep in possible_separators})
        separator = max(counts, key=lambda k: counts[k]) if counts else ","

        return encoding, separator

    def calculate_usability(self, metadata: dict[str, Any]) -> float:
        score = 0
        max_score = 6
        if metadata["license"]["name"] != "Unknown":
            score += 1
        if metadata["alternateName"] != "":
            score += 1
        if metadata["description"] != "":
            score += 1
        if len(metadata["keywords"]) != 0:
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
            else:
                continue
            break
        return round(score / max_score, 2)

    def process_dataset(self, path: Path) -> None:  # noqa: C901
        # get metadata
        metadata_path = path / "croissant_metadata.json"
        with metadata_path.open(encoding="utf-8") as file:
            metadata: dict[str, Any] = json.load(file)
        records: list[dict[str, Any]] = metadata["recordSet"]

        score = self.calculate_usability(metadata)
        metadata["usability"] = score

        for file_record in records:
            csv_file = path / file_record["@id"].replace("+", " ").replace("/", "_")
            encoding, separator = self.analyze_csv_file(csv_file)
            df = pd.read_csv(
                csv_file, encoding=encoding, sep=separator, engine="python", on_bad_lines="skip"
            )

            # remove unnecessary spaces
            df.columns = df.columns.str.strip()
            for i, column in enumerate(file_record["field"]):
                data_type = column["dataType"][0].rsplit(":", 1)[-1].lower()
                column_name = column["name"]
                # catch case where column has empty name
                if column_name == "":
                    column_name = f"Unnamed: {i}"
                data = df[column_name].dropna().to_list()
                # case for numeric columns
                if data_type in ["int", "integer", "float"]:
                    if isinstance(data[0], str):
                        try:
                            # catch case where 1923423 = "1,923,423"
                            # NOTE: This causes issues with German-style decimal separators
                            data = [float(d.replace(",", "")) for d in data]
                        except Exception:
                            # map strings to numbers
                            unique_strings = list(set(data))
                            mapping = {string: idx for idx, string in enumerate(unique_strings)}
                            data = [mapping[item] for item in data]
                    # create histogram
                    densities, bins = np.histogram(data, density=True, bins=self.bin_count)
                    # save hist
                    column["histogram"] = {
                        "bins": list(bins),
                        "densities": list(densities / np.sum(densities)),
                    }
                    column["statistics"] = df[column_name].dropna().describe().to_dict()
                # case for text columns
                elif data_type == "text":
                    n_unique = len(set(data))
                    top_10 = dict(Counter(data).most_common(10))
                    column["n_unique"] = n_unique
                    column["most_common"] = top_10
                elif data_type == "boolean":
                    if isinstance(data[0], str):
                        data = [d.lower() for d in data]
                        if data[0] == "true" or data[0] == "false":
                            data = [d == "true" for d in data]
                        elif data[0] == "yes" or data[0] == "no":
                            data = [d == "yes" for d in data]
                        elif data[0] == "t" or data[0] == "f":
                            data = [d == "t" for d in data]
                    elif isinstance(data[0], int | float):
                        max_value = max(data)
                        data = [d == max_value for d in data]
                    n_true = sum(data)
                    n_false = len(data) - n_true
                    column["n_true"] = n_true
                    column["n_false"] = n_false

        # write metadata to target_dir
        file_name = "/".join(str(path).split("/")[-2:]).replace("/", "_") + ".json"
        with (self.target_dir / file_name).open("w") as file:
            json.dump(metadata, file, indent=4, ensure_ascii=False)

    def start(self) -> None:
        process_list: list[Path] = []
        for path in self.source_dir.rglob("croissant_metadata.json"):
            if len(list(path.parent.iterdir())) > 1:
                process_list.append(path.parent)
        process_list = process_list[: min(self.max_count, len(process_list))]
        with tqdm.tqdm(total=len(process_list), desc="processing datasets") as progress:
            for dataset_path in process_list:
                try:
                    self.process_dataset(dataset_path)
                except Exception as e:
                    print(f"Error occurred with {dataset_path}: {e}")
                    self.error_count += 1
                progress.update(1)
        print(
            f"{len(process_list) - self.error_count} of {len(process_list)} datasets processed"
            f"({round((len(process_list) - self.error_count) / len(process_list) * 100)}%)"
        )


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="create histograms for kaggle datasets")
    parser.add_argument(
        "--source",
        type=str,
        default="../kaggle_metadata",
        help="path to metadata (default %(default)s)",
    )
    parser.add_argument(
        "--result",
        type=str,
        default="../croissant",
        help="path to result dir (default %(default)s)",
    )
    parser.add_argument(
        "--max-datasets",
        type=int,
        default=10**1000,
        help="max count of datasets to be processed (default %(default)s)",
    )
    parser.add_argument(
        "--bin-count",
        type=int,
        default=10,
        help="number of bins per histogram (default %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    result_dir = Path(args.result)
    source_dir = Path(args.source)

    if not source_dir.exists():
        print("This program requires a directory with croissant metadata to work!")
        sys.exit(1)
    result_dir.mkdir(exist_ok=True)

    creator = HistogramCreator(
        source_dir=source_dir,
        target_dir=result_dir,
        max_count=args.max_datasets,
        bin_count=args.bin_count,
    )
    creator.start()


if __name__ == "__main__":
    main()
