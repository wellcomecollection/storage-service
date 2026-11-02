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

COMPOSE_FILE="$PROJECT_DIRECTORY/docker-compose.yml"

if [[ ! -f "$COMPOSE_FILE" ]]
then
  ./builds/run_sbt_task_in_docker.sh "project $PROJECT_NAME" "test"
  exit 0
fi

# Start the project's service mocks on the host; tests reach them on the ports in the compose file.
# Prefer the Compose v2 plugin; fall back to the standalone v1 binary where only that is installed.
if docker compose version > /dev/null 2>&1
then
  COMPOSE=(docker compose)
else
  COMPOSE=(docker-compose)
fi

compose() {
  "${COMPOSE[@]}" --project-name "sbt-tests-$PROJECT_NAME" --file "$COMPOSE_FILE" "$@"
}

compose up --detach

set +o errexit
./builds/run_sbt_task_in_docker.sh "project $PROJECT_NAME" "test"
status=$?
set -o errexit

if (( status != 0 ))
then
  echo "*** Tests failed, showing the service logs"
  compose logs --no-color --tail 300
fi

compose down --volumes
exit $status
