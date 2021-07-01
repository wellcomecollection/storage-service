#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

PROJECT="$1"

PROJECT_DIRECTORY=$(jq -r .folder ".sbt_metadata/$PROJECT.json")

CURRENT_COMMIT=$(git rev-parse HEAD)

ROOT=$(git rev-parse --show-toplevel)
BUILDS_DIR="$ROOT/builds"

$BUILDS_DIR/run_sbt_task_in_docker.sh "project $PROJECT" ";stage"

docker build \
  --file "$PROJECT_DIRECTORY/Dockerfile" \
  --tag "$PROJECT:$CURRENT_COMMIT" \
  "$PROJECT_DIRECTORY"

mkdir -p "$ROOT/.releases"
echo "$CURRENT_COMMIT" >> "$ROOT/.releases/$PROJECT"
