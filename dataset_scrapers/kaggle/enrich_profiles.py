from __future__ import annotations

import argparse
import json
import math
import multiprocessing as mp
import sys
import time
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any
from urllib.parse import unquote

import cchardet
import numpy as np
import pandas as pd
from pandas import Series
from tqdm import tqdm

if TYPE_CHECKING:
    from multiprocessing.sharedctypes import Synchronized

error_count: Synchronized[int]


class ErrorType(Enum):
    File = 0
    Column = 1
    Dataset = 2


def init_workers(counter: Synchronized[int]) -> None:
    """Initialize each worker with a global synchronized counter."""
    global error_count  # noqa: PLW0603
    error_count = counter


class HistogramCreator:
    def __init__(
        self,
        source_dir: Path,
        target_dir: Path,
        error_dir: Path,
        max_count: int,
        bin_count: int = 10,
        workers: int = mp.cpu_count(),
    ) -> None:
        self.source_dir = source_dir
        self.target_dir = target_dir
        self.error_dir = error_dir
        self.max_count = max_count
        self.bin_count = bin_count
        self.num_processes = workers
        self.error_dir.mkdir(parents=True, exist_ok=True)

    def analyze_csv_file(self, path: Path, n_columns: int) -> tuple[str, str]:
        """Analyze a CSV file and return its encoding and separator."""
        candidates = [",", ";", "\t", "|"]

        with path.open("rb") as file:
            result = cchardet.detect(file.read())
        encoding = result["encoding"]

        with path.open("r", encoding=encoding) as file:
            first = file.readline().strip()
        separator = ","
        for sep in candidates:
            n_elements = len([s for s in first.split(sep) if s])
            if n_elements == n_columns:
                separator = sep

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

    def process_numerical(self, data: Series, column: dict[str, Any]) -> None:
        if data.dtype == "object":
            try:
                # catch case where 1923423 = "1,923,423"
                # NOTE: This causes issues with German-style decimal separators
                data = data.str.replace(",", "").astype(float)
            except Exception:
                # map strings to numbers
                mapping = {string: idx for idx, string in enumerate(data.unique())}
                data = Series([mapping[item] for item in data])
        # create histogram
        densities, bins = np.histogram(
            data, density=True, bins=min(data.nunique(), self.bin_count)
        )
        # save hist
        column["histogram"] = {
            "bins": list(bins),
            "densities": list(densities / np.sum(densities)),
        }
        statistics = data.describe().to_dict()
        for key in list(statistics.keys()):
            value = statistics[key]
            if pd.isna(value) or (isinstance(value, float) and math.isnan(value)):
                statistics[key] = "NaN"
            if key == "25%":
                statistics["firstQuartile"] = value
                statistics.pop(key)
            elif key == "50%":
                statistics["secondQuartile"] = value
                statistics.pop(key)
            elif key == "75%":
                statistics["thirdQuartile"] = value
                statistics.pop(key)
        column["statistics"] = statistics

    def process_text(self, data: Series, column: dict[str, Any]) -> None:
        n_unique = data.nunique()
        top_10 = data.value_counts().head(10).to_dict()
        column["nUnique"] = n_unique
        column["mostCommon"] = top_10

    def process_bool(self, data: Series, column: dict[str, Any]) -> None:
        counts = data.value_counts().to_dict()
        column["counts"] = counts

    def process_date(self, data: Series, column: dict[str, Any]) -> None:
        # NOTE: Using mixed format is risky and can lead to false date parsing
        try:
            data = pd.to_datetime(data, format="mixed", dayfirst=True, utc=True)
        except Exception:
            # fallback to general text processing
            column["dataType"] = ["sc:Text"]
            self.process_text(data, column)
            return
        min_date, max_date = data.min(), data.max()
        unique_dates = data.nunique()
        column["minDate"] = min_date.isoformat()
        column["maxDate"] = max_date.isoformat()
        column["uniqueDates"] = unique_dates

    def handle_exception(self, path: Path, e: Exception, mode: int) -> None:
        print(f"Error occurred with {path}: {e}", flush=True)
        with error_count.get_lock():
            error_id = error_count.value
            error_count.value += 1
        mode_header = ErrorType(mode).name
        filename = self.error_dir / f"error_{error_id}.log"
        with Path.open(filename, "w") as f:
            f.write(
                mode_header + ";" + str(type(e).__name__) + ";" + str(e).strip() + ";" + str(path)
            )

    def get_file_paths(self, metadata: dict[str, Any]) -> list[Path]:
        """Get all file paths from the metadata."""
        paths = []
        for file in metadata["distribution"]:
            if "contentUrl" in file:
                url: str = file["contentUrl"]
                if url.endswith((".csv", ".tsv")):
                    path = Path(file["contentUrl"])
                    paths.append(path)
        return paths

    def process_dataset(self, path: Path) -> None:  # noqa: C901
        # open metadata file
        metadata_path = path / "croissant_metadata.json"
        with metadata_path.open(encoding="utf-8") as file:
            metadata: dict[str, Any] = json.load(file)
        records: list[dict[str, Any]] = metadata["recordSet"]
        paths = self.get_file_paths(metadata)
        try:
            assert len(paths) == len(records), "Number of csv paths and records do not match"
        except AssertionError as e:
            self.handle_exception(path, e, 2)
            return
        # calculate usability
        score = self.calculate_usability(metadata)
        metadata["usability"] = score
        # iterate through each file
        for i, file_record in enumerate(records):
            try:
                filepath = paths[i]
                csv_file = path / filepath
                if not csv_file.exists():
                    # fallback to old method
                    csv_file = path / unquote(
                        file_record["@id"].replace("+", " ").replace("/", "_")
                    )
                encoding, separator = self.analyze_csv_file(csv_file, len(file_record["field"]))
                table = pd.read_csv(
                    csv_file,
                    encoding=encoding,
                    sep=separator,
                    engine="python",
                    on_bad_lines="skip",
                )
                assert len(table.columns) >= len(file_record["field"]), (
                    f"Number of columns and fields do not match: {csv_file}"
                )
            except Exception as e:
                self.handle_exception(path, e, 0)
                continue
            # remove unnecessary spaces
            table.columns = table.columns.str.strip()
            # iterate through each column
            for j, column in enumerate(file_record["field"]):
                try:
                    data_type = column["dataType"][0].rsplit(":", 1)[-1].lower()
                    data = table.iloc[:, j].dropna()
                    if data_type in ["int", "integer", "float"]:
                        self.process_numerical(data, column)
                    elif data_type == "text":
                        self.process_text(data, column)
                    elif data_type == "boolean":
                        self.process_bool(data, column)
                    elif data_type == "date":
                        self.process_date(data, column)
                except Exception as e:
                    self.handle_exception(path, e, 1)
                    column["error"] = str(e)
                    continue

        # write metadata to target_dir
        file_name = "/".join(str(path).split("/")[-2:]).replace("/", "_") + ".json"
        try:
            with (self.target_dir / file_name).open("w") as file:
                json.dump(metadata, file, indent=4, ensure_ascii=False, allow_nan=False)
        except Exception as e:
            self.handle_exception(path, e, 0)
            print("NaN error detected with metadata: ", metadata_path)
            return

    def merge_errors(self) -> None:
        lines: list[str] = []
        for error_file in self.error_dir.iterdir():
            if error_file.is_file():
                with error_file.open("r", encoding="utf-8") as f:
                    lines.extend(f.readlines())
        lines = [line.replace("\n", "") for line in lines]
        lines.sort()
        output_file = Path("../error_list.log")
        output_file.write_text("\n".join(lines), encoding="utf-8")

    def calculate_kaggle_path(self, path: Path, base_dir: Path) -> str:
        path = path.relative_to(base_dir.parent)
        filename = path.name
        same_names: list[Path] = []
        for p in base_dir.rglob(filename):
            p_relative = p.relative_to(base_dir.parent)
            if p_relative != path:
                same_names.append(p_relative)
        # no conflicts with other paths, return just the filename
        if len(same_names) == 0:
            return filename
        # search for earliest folder where paths diverge
        i = 0
        while all(p.parts[i] == path.parts[i] for p in same_names):
            i += 1
        return str(Path(*path.parts[i:])).replace("/", "_")

    def start(self) -> None:
        dataset_paths = [
            path.parent
            for path in self.source_dir.rglob("croissant_metadata.json")
            if len(list(path.parent.iterdir())) > 1
        ]
        dataset_paths = dataset_paths[: min(self.max_count, len(dataset_paths))]
        n_datasets = len(dataset_paths)

        error_count = mp.Value("I", 0)
        with mp.Pool(
            processes=self.num_processes, initializer=init_workers, initargs=(error_count,)
        ) as pool:
            list(tqdm(pool.imap_unordered(self.process_dataset, dataset_paths), total=n_datasets))

        self.merge_errors()
        print(f"{error_count.value} errors occurred while processing {n_datasets} datasets")


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
        "--error-dir",
        type=str,
        default="../errors",
        help="path to error dir (default %(default)s)",
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
    parser.add_argument(
        "-w",
        "--workers",
        type=int,
        default=mp.cpu_count(),
        help="number of workers to use (default %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    start = time.perf_counter()
    mp.set_start_method("spawn")
    args = parse_args()
    result_dir = Path(args.result)
    source_dir = Path(args.source)
    error_dir = Path(args.error_dir)

    if not source_dir.exists():
        print("This program requires a directory with croissant metadata to work!")
        sys.exit(1)
    result_dir.mkdir(exist_ok=True)

    creator = HistogramCreator(
        source_dir=source_dir,
        target_dir=result_dir,
        error_dir=error_dir,
        max_count=args.max_datasets,
        bin_count=args.bin_count,
        workers=args.workers,
    )
    creator.start()
    print(f"Finished in {time.perf_counter() - start:.2f} seconds.")


if __name__ == "__main__":
    main()
