#!/usr/bin/env bash

set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-3.7.0}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
KAFKA_HOME="${KAFKA_HOME:-${PROJECT_ROOT}/tools/kafka_${SCALA_VERSION}-${KAFKA_VERSION}}"
KAFKA_CONFIG="${KAFKA_CONFIG:-${KAFKA_HOME}/config/kraft/server.properties}"

if [[ ! -d "$KAFKA_HOME" ]]; then
    echo "Kafka installation not found at: $KAFKA_HOME" >&2
    echo "Run scripts/install_kafka.sh first." >&2
    exit 1
fi

if [[ ! -f "$KAFKA_CONFIG" ]]; then
    echo "Kafka configuration not found: $KAFKA_CONFIG" >&2
    exit 1
fi

LOG_DIRECTORY="$(
    awk -F= '
        /^[[:space:]]*log\.dirs[[:space:]]*=/ {
            value = $2
            gsub(/[[:space:]]/, "", value)
            print value
            exit
        }
    ' "$KAFKA_CONFIG"
)"

if [[ -z "$LOG_DIRECTORY" ]]; then
    echo "Could not determine log.dirs from $KAFKA_CONFIG" >&2
    exit 1
fi

META_PROPERTIES="${LOG_DIRECTORY}/meta.properties"

if [[ -f "$META_PROPERTIES" ]]; then
    echo "Kafka storage is already initialized:"
    echo "$META_PROPERTIES"
else
    KAFKA_CLUSTER_ID="$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)"

    echo "Formatting Kafka storage..."
    echo "Cluster ID: $KAFKA_CLUSTER_ID"

    "$KAFKA_HOME/bin/kafka-storage.sh" format \
        --cluster-id "$KAFKA_CLUSTER_ID" \
        --config "$KAFKA_CONFIG"

    echo "Kafka storage initialized."
fi

echo "Starting Kafka..."
exec "$KAFKA_HOME/bin/kafka-server-start.sh" "$KAFKA_CONFIG"