#!/bin/bash

if [ ! "$(docker ps -aqf name=mongodb)" ]; then
    parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" || exit; pwd -P | xargs dirname )

    docker run -d --name mongodb \
        --network mongo-network \
        -p 27017:27017 \
        -v "$parent_path"/mongodb:/data/db \
        -e MONGO_INITDB_ROOT_USERNAME="$MONGO_USER" \
        -e MONGO_INITDB_ROOT_PASSWORD="$MONGO_PW" \
        -e MONGO_INITDB_DATABASE="$MONGO_DBNAME" \
        mongo:6
else
    docker start mongodb
fi
