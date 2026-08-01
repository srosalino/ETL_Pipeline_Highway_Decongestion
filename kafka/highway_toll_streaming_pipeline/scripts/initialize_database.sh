#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SCHEMA_FILE="${PROJECT_ROOT}/sql/create_schema.sql"
ENV_FILE="${PROJECT_ROOT}/.env"

MYSQL_CONTAINER="${MYSQL_CONTAINER:-toll-mysql}"

if [[ ! -f "$ENV_FILE" ]]; then
    echo "Environment file not found: $ENV_FILE" >&2
    exit 1
fi

set -a
source "$ENV_FILE"
set +a

MYSQL_USER="${MYSQL_USER:-root}"

if [[ -z "${MYSQL_PASSWORD:-}" ]]; then
    echo "MYSQL_PASSWORD is not defined in $ENV_FILE" >&2
    exit 1
fi

if [[ ! -f "$SCHEMA_FILE" ]]; then
    echo "Schema file not found: $SCHEMA_FILE" >&2
    exit 1
fi

if ! docker inspect "$MYSQL_CONTAINER" >/dev/null 2>&1; then
    echo "MySQL container not found: $MYSQL_CONTAINER" >&2
    exit 1
fi

if [[ "$(docker inspect -f '{{.State.Running}}' "$MYSQL_CONTAINER")" != "true" ]]; then
    echo "MySQL container is not running: $MYSQL_CONTAINER" >&2
    exit 1
fi

echo "Initializing MySQL schema in container: $MYSQL_CONTAINER"

docker exec \
    -e MYSQL_PWD="$MYSQL_PASSWORD" \
    -i "$MYSQL_CONTAINER" \
    mysql \
    --user="$MYSQL_USER" \
    < "$SCHEMA_FILE"

echo "Database schema initialized successfully."