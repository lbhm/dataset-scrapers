#!/bin/bash

docker run -it --rm --network mongo-network mongo:6 \
    mongosh --host mongodb \
            -u "$MONGO_USER" \
            -p "$MONGO_PW" \
            --authenticationDatabase admin \
            "$MONGO_DBNAME"
