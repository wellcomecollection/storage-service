#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

PROJECT="$1"

PROJECT_DIRECTORY=$(jq -r .folder ".sbt_metadata/$PROJECT.json")

ROOT=$(git rev-parse --show-toplevel)
BUILDS_DIR="$ROOT/builds"

if [[ -f "$PROJECT_DIRECTORY/docker-compose.yml" ]]
then
  TEST_COMMAND=";dockerComposeUp;test;dockerComposeStop"
else
  TEST_COMMAND="test"
fi

$BUILDS_DIR/run_sbt_task_in_docker.sh "project $PROJECT" "$TEST_COMMAND"
