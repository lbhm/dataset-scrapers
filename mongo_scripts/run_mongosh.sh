#!/bin/bash

docker run -it --rm --network "${MONGO_NETWORK:-mongo-network}" mongo:6 \
    mongosh --host "${MONGO_CONTAINER:-mongodb}" \
            -u "$MONGO_USER" \
            -p "$MONGO_PW" \
            --authenticationDatabase admin \
            --eval "disableTelemetry()" \
            --shell \
            "${MONGO_DBNAME:-datasets}"
