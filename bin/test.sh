#!/usr/bin/env bash
set -e

CURRENT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

source "${CURRENT_DIR}/_common_setup.sh"

echo "running tests with 'pytest'..."
poetry run pytest -vv --cov=gen3analysis --cov=migrations/versions --cov-report term-missing:skip-covered --cov-report xml
