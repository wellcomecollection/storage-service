#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

NAME="$1"

CURRENT_COMMIT=$(git rev-parse HEAD)

FOLDER=$(jq -r .folder ".sbt_metadata/$NAME.json")

docker build --file "$FOLDER/Dockerfile" --tag "$NAME:$CURRENT_COMMIT" "$FOLDER"
