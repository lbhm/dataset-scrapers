#!/usr/bin/env python3
import nbformat
import os
import sys

"""
This script removes all metadata and cell output from a given notebook in an
in-place manner.

It expects a single argument with the notebook name to be stripped.
"""

if len(sys.argv) != 2:
    print("Usage: strip_notebook.py [NOTEBOOK]")
    sys.exit()

input = sys.argv[1]
if os.path.splitext(input)[1] != ".ipynb":
    print(f"Expected an Jupyter notebook with a .ipynb file extension, got {os.path.splitext(input)[1]}.")
    sys.exit()

# Read notebook
nb = nbformat.read(input, as_version=4)

# Remove all cell output and metadata
for cell in nb.cells:
    cell.metadata = {}
    if cell.cell_type == "code":
        cell.outputs = []
        cell["execution_count"] = None

# Remove notebook-wide metadata
nb.metadata = {}

# Write notebook to input file
nbformat.write(nb, input)
