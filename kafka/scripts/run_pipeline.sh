#!/usr/bin/env bash

set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

ENV_FILE="${PROJECT_ROOT}/.env"
REQUIREMENTS_FILE="${PROJECT_ROOT}/requirements.txt"
CONSUMER_SCRIPT="${PROJECT_ROOT}/src/streaming_data_reader.py"
PRODUCER_SCRIPT="${PROJECT_ROOT}/src/toll_traffic_generator.py"

# Select a working Python interpreter
if command -v py >/dev/null 2>&1 && py -3.12 --version >/dev/null 2>&1; then
    PYTHON=(py -3.12)
elif command -v python3 >/dev/null 2>&1 &&
     python3 --version >/dev/null 2>&1; then
    PYTHON=(python3)
elif command -v python >/dev/null 2>&1 &&
     python --version >/dev/null 2>&1; then
    PYTHON=(python)
else
    echo "Error: no usable Python installation was found." >&2
    echo "Install Python or make it available through py, python3, or python." >&2
    exit 1
fi

echo "Using Python interpreter:"
"${PYTHON[@]}" --version

if [[ -f "$ENV_FILE" ]]; then
    set -a
    # shellcheck disable=SC1090
    source "$ENV_FILE"
    set +a
else
    echo "Warning: .env file not found."
    echo "Copy .env.example to .env and add your credentials."
fi

if [[ -z "${MYSQL_PASSWORD:-}" ]]; then
    echo "MYSQL_PASSWORD is not set." >&2
    exit 1
fi

if [[ ! -f "$REQUIREMENTS_FILE" ]]; then
    echo "Requirements file not found: $REQUIREMENTS_FILE" >&2
    exit 1
fi

if [[ ! -f "$CONSUMER_SCRIPT" ]]; then
    echo "Consumer script not found: $CONSUMER_SCRIPT" >&2
    exit 1
fi

if [[ ! -f "$PRODUCER_SCRIPT" ]]; then
    echo "Producer script not found: $PRODUCER_SCRIPT" >&2
    exit 1
fi

echo "Installing Python dependencies..."
"${PYTHON[@]}" -m pip install --requirement "$REQUIREMENTS_FILE"

CONSUMER_PID=""

cleanup() {
    if [[ -n "$CONSUMER_PID" ]] &&
       kill -0 "$CONSUMER_PID" 2>/dev/null; then
        echo "Stopping streaming consumer..."
        kill "$CONSUMER_PID"
        wait "$CONSUMER_PID" || true
    fi
}

trap cleanup EXIT INT TERM

echo "Starting streaming consumer..."
"${PYTHON[@]}" "$CONSUMER_SCRIPT" &
CONSUMER_PID=$!

sleep 3

# Check whether the consumer terminated during startup.
if ! kill -0 "$CONSUMER_PID" 2>/dev/null; then
    echo "Error: the streaming consumer terminated during startup." >&2
    wait "$CONSUMER_PID"
    exit 1
fi

echo "Starting toll traffic generator..."
"${PYTHON[@]}" "$PRODUCER_SCRIPT" \
    --message-count "${MESSAGE_COUNT:-1000}" \
    --max-delay "${MAX_DELAY_SECONDS:-2}"

echo "Producer completed."