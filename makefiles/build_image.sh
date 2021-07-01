#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

NAME="$1"
FILE="$2"

CURRENT_COMMIT=$(git rev-parse HEAD)

docker build --file "$FILE" --tag "$NAME:$CURRENT_COMMIT" $(dirname "$FILE")
