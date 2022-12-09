#!/bin/bash

container="${MONGO_CONTAINER:-mongodb}"

if [ ! "$(docker ps -aqf name="${container}")" ]; then
    parent_path=$( cd "$(dirname "${BASH_SOURCE[0]}")" || exit; pwd -P | xargs dirname )

    docker run -d --name "${container}" \
        --network "${MONGO_NETWORK:-mongo-network}" \
        -p "${MONGO_PORT:-27017}":27017 \
        -v "$parent_path/${MONGO_DATADIR:-mongodb}":/data/db \
        -e MONGO_INITDB_ROOT_USERNAME="${MONGO_USER:?Username not set.}" \
        -e MONGO_INITDB_ROOT_PASSWORD="${MONGO_PW:?Password not set.}" \
        -e MONGO_INITDB_DATABASE="${MONGO_DBNAME:-datasets}" \
        mongo:6
else
    docker start "${container}"
fi
