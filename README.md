# dataset-scrapers

![Python Version](https://img.shields.io/python/required-version-toml?tomlFilePath=https%3A%2F%2Fraw.githubusercontent.com%2Flbhm%2Fdataset-scrapers%2Fmain%2Fpyproject.toml)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)
![GitHub License](https://img.shields.io/github/license/lbhm/dataset-scrapers)

This repository contains scripts for downloading, analyzing, and enriching dataset profile collections.
It provides the basis for an experimental evaluation of novel research ideas in the field of metadata-driven
dataset search (also known as dataset search over decentralized data repositories).
The scope of this repository includes dataset collections that are:

- publicly available
- provide metadata in a standardized format (e.g., [Croissant](https://github.com/mlcommons/croissant))
- make the raw data available for download so that we can enrich dataset profiles with additional information (such as synopses)

The scripts for each dataset collection are in their own directory with joint utilities located in `dataset_scrapers/`.
More details on our profile enrichment and scraping statistics are located in `docs/`.

## Setup

All scripts are written in Python with the dependencies specified in `pyproject.toml`.
We recommend using [`uv`](https://docs.astral.sh/uv/) to install and manage the project dependencies.
To set up a new virtual environment, clone the repository and run `uv sync`.
After that, the virtual environment is available at `.venv/bin/activate`.

Instructions for how to reproduce a dataset collection are located in `docs/`.

## Dataset Collections

### Available

- [Kaggle](https://www.kaggle.com/datasets)

### Roadmap

- [OpenML](https://www.openml.org/search?type=data&sort=runs&status=active)
