#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

if (( $# == 1 ))
then
  PROJECT="$1"
  PROJECT_DIRECTORY="$1"
elif (( $# == 2 ))
then
  PROJECT="$1"
  PROJECT_DIRECTORY="$2"
else
  echo "Usage: publish_sbt_app.sh <PROJECT> [<PROJECT_DIRECTORY>]" >&2
  exit 1
fi

ROOT=$(git rev-parse --show-toplevel)
BUILDS_DIR="$ROOT/builds"

$BUILDS_DIR/build_sbt_image.sh "$PROJECT" "$PROJECT_DIRECTORY"
$BUILDS_DIR/publish_image_with_weco_deploy.sh "$PROJECT"
