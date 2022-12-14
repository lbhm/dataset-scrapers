# dataset-scraper

![Python](https://img.shields.io/badge/Python-v3.10-green?logo=python)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

Notebooks for scraping, analyzing, and parsing dataset collections.

In addition, there are scripts for running a MongoDB container and importing the
generated JSON files into a MongoDB collection.

## Setup

The notebooks and scripts assume the existance of the following environment variables.
Except for the username and password, default values are used if the values do not exist.

```bash
MONGO_USER
MONGO_PW
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DBNAME=datasets
MONGO_CONTAINER=mongodb
MONGO_NETWORK=mongo-network
MONGO_DATADIR=mongodb
```

You can write them into a `.env` file so that they are ignore by Git and load the file
with

```bash
export $(cat .env | xargs)
```

## Done

- OpenML

## TODO

- [SNAP](https://snap.stanford.edu/data/index.html)
- Kaggle

## How-To

### Copy Data Collections

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

<details><summary>More complex queries can be composed like this.</summary>
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

Reference at [mongodb.com](https://www.mongodb.com/docs/manual/tutorial/query-array-of-documents/#a-single-nested-document-meets-multiple-query-conditions-on-nested-fields)
