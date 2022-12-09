# dataset-analyzer

![Python](https://img.shields.io/badge/Python-v3.10-green?logo=python)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Scripts for scraping, analyzing, and parsing dataset collections.

## Setup

The scripts assume the existance of the following environment variables:

```bash
MONGO_HOST
MONGO_PORT
MONGO_USER
MONGO_PW
MONGO_DBNAME
```

You can write them into a `.env` file so that they are ignore by Git and load the file with

```bash
export $(cat .env | xargs)
```

## Done

- OpenML

## TODO

- SNAP
- Kaggle

## How-To

### Copy Data Collections

```bash
rsync --recursive --progress SOURCE DEST
```

A trailing slash on the source avoids creating an additional directory level at the destination.
