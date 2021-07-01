#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

PROJECT="$1"

PROJECT_DIRECTORY=$(jq -r .folder ".sbt_metadata/$PROJECT.json")

ROOT=$(git rev-parse --show-toplevel)
BUILDS_DIR="$ROOT/builds"

if [[ -f "$PROJECT_DIRECTORY/Dockerfile" ]]
then
  $BUILDS_DIR/build_sbt_image.sh "$PROJECT"
  $BUILDS_DIR/publish_image_with_weco_deploy.sh "storage" "$PROJECT"
else
  echo "No Dockerfile, nothing to build!"
fi
