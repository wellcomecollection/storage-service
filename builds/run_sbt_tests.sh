#!/usr/bin/env bash
<<EOF
Run tests for an sbt project.

This script will automatically detect whether we need to start containers
using Docker Compose as part of these tests.

This script is mirrored across our Scala repos.

== Usage ==

Pass the name of the sbt project as a single argument, e.g.

    $ run_sbt_tests.sh file_indexer
    $ run_sbt_tests.sh snapshot_generator
    $ run_sbt_tests.sh transformer_mets

EOF

set -o errexit
set -o nounset

if (( $# == 1))
then
  PROJECT_NAME="$1"
else
  echo "Usage: run_sbt_tests.sh <PROJECT>" >&2
  exit 1
fi

PROJECT_DIRECTORY=$(./builds/get_sbt_project_directory.sh "$PROJECT_NAME")

echo "*** Running sbt tests"

if [[ -f "$PROJECT_DIRECTORY/docker-compose.yml" ]]
then
  ./builds/run_sbt_task_in_docker.sh "project $PROJECT_NAME" ";dockerComposeUp;test;dockerComposeStop"
else
  ./builds/run_sbt_task_in_docker.sh "project $PROJECT_NAME" "test"
fi
