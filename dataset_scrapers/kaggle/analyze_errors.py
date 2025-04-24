import argparse
from collections import Counter
from pathlib import Path


def analyze_most_common(strings: list[str], n: int = 10) -> None:
    counter = Counter(strings)
    total = sum(counter.values())
    most_common = [(category, count, count / total) for category, count in counter.most_common(n)]
    for category, count_abs, count_rel in most_common:
        print(f"{category}: {count_abs} occurrences ({count_rel:.2%})")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="analyze kaggle metadata errors")
    parser.add_argument(
        "--error-path",
        type=str,
        default="../error_list.log",
        help="path to error_list.log (default %(default)s)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    error_path = Path(args.error_path)
    column_errors: list[str] = []
    file_errors: list[str] = []

    with Path.open(error_path, "r") as f:
        for line in f:
            error = line.strip()
            parts = error.split(";")
            if parts[0] == "Column":
                column_errors.append(parts[1])
            else:
                file_errors.append(parts[1])
    print("Column errors:")
    analyze_most_common(column_errors)
    print("\nFile errors:")
    analyze_most_common(file_errors)


if __name__ == "__main__":
    main()
