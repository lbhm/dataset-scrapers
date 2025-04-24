# Instructions for Reproducing the Kaggle Corpus

## 0. Get Access to the Kaggle API

Follow the steps provided [`here`](https://www.kaggle.com/docs/api) under "Authentication".

## 1. Download the Metadata

Corresponding script: `kaggle/download_metadata.py`

Available arguments:

- keyword `-k`or `--keyword` (string): Metadata will be downloaded from Kaggle matching this keyword. Defaults to all metadata available from the [Meta Kaggle](https://www.kaggle.com/datasets/kaggle/meta-kaggle) dataset.
- start index `-i` or `--start-index` (integer): Used to continue the download from a certain point. Defaults to 0.
- workers `-w` or `--workers`(integer): If set to >1, the script will use multithreading to download multiple metadata at the same time. Defaults to 1. Note: The Kaggle API has rate limits, so be careful with this setting.
- max pages `--max-pages` (integer): Maximum number of pages to look for metadata if keyword is provided. Defaults to 100.
- data directory `--data-dir` (string): Desired path where the data directory should be created, which is mainly used to save the metakaggle dataset and references to datasets where errors occurred during download. Defaults to `../data`.
- metadata directory `--output` (string): Desired path to the directory where the metadata will be collected. Defaults to `../kaggle_metadata`

## 2. Analyze the Metadata (optional)

Corresponding script: `kaggle/analyze_metadata.py`

Available arguments:

- source dir `--source` (string): Path to the metadata to be analyzed. Defaults to `../kaggle_metadata`.
- output dir `--output` (string): Desired path to the directory where the plots will be saved. Defaults to `../plots`.
- max files `--max-files` (integer): Upper limit for the x-Axis in the plot showing the distribution of CSV files in a dataset. Defaults to 100.
- max size `--max-size` (integer): Upper limit for the x-Axis in the plot showing the distribution of file sizes of .zip dataset files.
- max columns `--max-columns` (integer): Upper limit for the x-Axis in the plot showing the distribution of columns in csv files.
- show plots `--show-plots` (bool): Whether all plots should be rendered after analysis. Defaults to `false`.

## 3. Download Datasets

Corresponding script: `kaggle/download_datasets.py`

Available arguments:

- path `--path` (string): Path to croissant files with metadata. The datasets will be downloaded into the same directories. Defaults to `../kaggle_metadata`.
- start index `-i` or `--start-index` (integer): Used to continue the download from a certain point. Defaults to 0.

## 4. Enrich Dataset Profiles

Corresponding script: `kaggle/enrich_profiles.py`

Available arguments:

- source dir `--source` (string): Path to metadata with datasets. Defaults to `../kaggle_metadata`.
- result dir `--result` (string): Desired path to the directory where the metadata enriched with histograms will be collected. Defaults to `../croissant`.
- max datasets `--max-datasets` (integer): Maximum number of datasets to be processed. Defaults to all datasets available.
- bin count `--bin-count` (integer): Number of bins used for every histogram. Defaults to 10.

## 5. Analyze Errors (optional)
Corresponding script: `kaggle/analyze_errors.py`

Available arguments:

- error path `--error-path` (string): Path to the `error_list.log` file created by the `enrich_profiles.py` script. Defaults to `../error_list.log`.
