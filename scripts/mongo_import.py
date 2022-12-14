#!/usr/bin/env python3
import argparse
import json
import os
from typing import Sequence

import pymongo
from pymongo.errors import DocumentTooLarge, OperationFailure

"""
Import all JSON files from a directory into a MongoDB collection.
"""


def main(argv: Sequence[str] | None = None) -> bool:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--host",
        type=str,
        default=os.getenv("MONGO_HOST", "localhost"),
        help="Connection string to a MongoDB instance (default: localhost)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=os.getenv("MONGO_PORT", 27017),
        help="Port of the MongoDB instance (default: 27017)",
    )
    parser.add_argument(
        "--user", "-u", type=str, default=os.getenv("MONGO_USER"), help="Username"
    )
    parser.add_argument(
        "--password", "-p", type=str, default=os.getenv("MONGO_PW"), help="Password"
    )
    parser.add_argument(
        "--database",
        "-d",
        type=str,
        default=os.getenv("MONGO_DBNAME", "datasets"),
        help="MongoDB database to connect to",
    )
    parser.add_argument(
        "--collection", "-c", type=str, required=True, help="Collection to import into"
    )
    parser.add_argument(
        "--files", "-f", type=str, required=True, nargs="+", help="A list of JSON files"
    )
    args = parser.parse_args(argv)

    if args.user is None or args.password is None:
        raise ValueError(
            f"User and PW must be specified, got {tuple(args.user, args.password)}."
        )

    client = pymongo.MongoClient(
        host=args.host, port=args.port, username=args.user, password=args.password
    )
    db = client[args.database]
    collection = db[args.collection]
    collection.drop()

    errors = []
    for file in args.files:
        with open(file, "r") as fp:
            try:
                _ = collection.insert_one(json.load(fp))
            except (DocumentTooLarge, OperationFailure) as ex:
                errors.append((file, ex))

    client.close()

    if errors:
        print(errors)
    else:
        print("OK")


if __name__ == "__main__":
    raise SystemExit(main())
