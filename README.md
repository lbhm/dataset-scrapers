# dataset-scraper

![Python](https://img.shields.io/badge/python-3.10%20--%203.12-informational)
![License](https://img.shields.io/badge/license-MIT-green)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Scripts for scraping, parsing, analyzing, and storing dataset collections.

In addition, there are scripts for running a MongoDB container and importing collections
of dataset profiles into a MongoDB collection.

## Setup

The notebooks and scripts assume the existance of the following environment variables.
Variables without a default value specified below must be defined first in order for the
scripts to work correctly.

```bash
MONGO_USER
MONGO_PW
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DBNAME=datasets
MONGO_CONTAINER=mongodb
MONGO_NETWORK=mongo-network
MONGO_DATADIR=mongodb

RAW_DATADIR=data
```

You can write them into a `.env` file so that they are ignored by Git and load the file
with

```bash
export $(cat .env | xargs)
```

You also need to install the dependencies specified in `requirements.txt` and configure
`pre-commit`.

```bash
virtualenv venv
source venv/bin/activate
pip install -r requirements.txt

pre-commit install
```

## Dataset Collections

### Supported

- [OpenML](https://www.openml.org/search?type=data&sort=runs&status=active)

### TODO

- [GitTables](https://gittables.github.io/)
- [SNAP](https://snap.stanford.edu/data/index.html)
- [Kaggle](https://www.kaggle.com/datasets)
- [Open Data Portal Watch](https://data.wu.ac.at/portalwatch)
  - Service unavailable as of June 1, 2023
  - Source code on [GitHub](https://github.com/sebneu/portalwatch)

## How-To

### Copy Document Collections From a Remote Machine

```bash
rsync --recursive --progress SOURCE DEST
```

A trailing slash on the source avoids creating an additional directory level at the
destination.

### Query an Array of Embedded Documents in MongoDB

```javascript
db.openml.find({
    name: "weather",
    features: {
        $elemMatch: {
            "name": "humidity",
            data_type: "numeric"
        },
        $elemMatch: {
            name: "windy",
            data_type: "nominal"
        }
    }
})
```

<details>
<summary>More complex queries can be composed like this.</summary>

```json
{
  "$and":[
    {
      "name":{
        "$regex":".*cancer.*",
        "$options":"i"
      }
    },
    {
      "attributes":{
        "$elemMatch":{
          "$and":[
            {
              "name":{
                "$eq":"age"
              }
            },
            {
              "dtype":{
                "$eq":"numeric"
              }
            }
          ]
        }
      }
    },
    {
      "attributes":{
        "$elemMatch":{
          "$and":[
            {
              "name":{
                "$regex":".*smoker.*",
                "$options":"i"
              }
            },
            {
              "$or":[
                {
                  "dtype":{
                    "$eq":"categorical"
                  }
                },
                {
                  "dtype":{
                    "$eq":"string"
                  }
                }
              ]
            },
            {
              "n_missing_values":{
                "$lte":10
              }
            }
          ]
        }
      }
    }
  ]
}
```

</details>

Reference at [mongodb.com](https://www.mongodb.com/docs/manual/tutorial/query-array-of-documents/#a-single-nested-document-meets-multiple-query-conditions-on-nested-fields).
