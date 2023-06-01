#!/usr/bin/env python3
import argparse
from typing import Sequence

import nbformat

"""
This script removes all metadata and cell output from a given list of notebooks
in an in-place manner.
"""


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("filenames", nargs="*", help="A list of iPython notebooks")
    args = parser.parse_args(argv)

    return_value = 0

    for filename in args.filenames:
        try:
            # Read notebook
            nb = nbformat.read(filename, as_version=4)

            # Remove all cell output and metadata
            stripped = False
            for cell in nb.cells:
                cell.metadata = {}
                if cell.cell_type == "code":
                    if cell.outputs:
                        cell.outputs = []
                        stripped = True
                    if cell["execution_count"]:
                        cell["execution_count"] = None
                        stripped = True

            # Remove notebook-wide metadata
            nb.metadata = {}

            # Write notebook to input file
            nbformat.write(nb, filename)

            if stripped:
                print(f"Stripped {filename}")
        except OSError as e:
            print(f"Could not open file {filename}: {e}")
            return_value = 100
        except nbformat.ValidationError as e:
            print(f"ValidationError in {filename}: {e}")
            return_value = 101

    return return_value


if __name__ == "__main__":
    raise SystemExit(main())
