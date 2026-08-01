#!/usr/bin/env bash

set -euo pipefail

KAFKA_VERSION="${KAFKA_VERSION:-3.7.0}"
SCALA_VERSION="${SCALA_VERSION:-2.12}"

KAFKA_ARCHIVE="kafka_${SCALA_VERSION}-${KAFKA_VERSION}.tgz"
KAFKA_DIRECTORY="kafka_${SCALA_VERSION}-${KAFKA_VERSION}"
KAFKA_URL="https://archive.apache.org/dist/kafka/${KAFKA_VERSION}/${KAFKA_ARCHIVE}"

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
INSTALL_DIRECTORY="${KAFKA_INSTALL_DIRECTORY:-${PROJECT_ROOT}/tools}"

mkdir -p "$INSTALL_DIRECTORY"
cd "$INSTALL_DIRECTORY"

if [[ -d "$KAFKA_DIRECTORY" ]]; then
    echo "Kafka is already installed at:"
    echo "${INSTALL_DIRECTORY}/${KAFKA_DIRECTORY}"
    exit 0
fi

echo "Downloading Kafka from:"
echo "$KAFKA_URL"

curl --fail --location --output "$KAFKA_ARCHIVE" "$KAFKA_URL"

echo "Extracting Kafka..."
tar -xzf "$KAFKA_ARCHIVE"

echo "Removing downloaded archive..."
rm "$KAFKA_ARCHIVE"

echo "Kafka installed successfully at:"
echo "${INSTALL_DIRECTORY}/${KAFKA_DIRECTORY}"