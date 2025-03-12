from collections import Counter
from pathlib import Path


def analyze_most_common(strings: list[str], n: int = 10) -> None:
    counter = Counter(strings)
    total = sum(counter.values())
    most_common = [(category, count, count / total) for category, count in counter.most_common(n)]
    for category, count_abs, count_rel in most_common:
        print(f"{category}: {count_abs} occurrences ({count_rel:.2%})")


def main() -> None:
    error_path = Path("/home/thinkemil/dataset-scrapers/dataset_scrapers/error_list.log")
    column_errors: list[str] = []
    file_errors: list[str] = []

    with Path.open(error_path, "r") as f:
        for line in f:
            error = line.strip()
            if error.startswith("Column"):
                column_errors.append(error[6:])
            else:
                file_errors.append(error[4:])
    print("Column errors:")
    analyze_most_common([error.split(";")[0] for error in column_errors])
    print("\nFile errors:")
    analyze_most_common([error.split(";")[0] for error in file_errors])


if __name__ == "__main__":
    main()
