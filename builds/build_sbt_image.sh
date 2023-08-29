#!/usr/bin/env bash
<<EOF
Build the Docker image for an sbt project.

This script will automatically detect the location of the Dockerfile based
on the sbt project config.

This script is mirrored across our Scala projects.

== Usage ==

Pass the name of the sbt project as arg 1, and the image tag as arg 2, e.g.

    $ build_sbt_image.sh file_indexer ref.19872ab
    $ build_sbt_image.sh snapshot_generator ref.1761817
    $ build_sbt_image.sh transformer_mets ref.9811987

EOF

set -o errexit
set -o nounset

if (( $# == 2))
then
  PROJECT_NAME="$1"
  IMAGE_TAG="$2"
else
  echo "Usage: build_sbt_image.sh <PROJECT> <IMAGE_TAG>" >&2
  exit 1
fi

PROJECT_DIRECTORY=$(./builds/get_sbt_project_directory.sh "$PROJECT_NAME")

echo "*** Building Docker image for sbt app"

./builds/run_sbt_task_in_docker.sh "project $PROJECT_NAME" ";stage"

docker build \
  --file "$PROJECT_DIRECTORY/Dockerfile" \
  --tag "$PROJECT_NAME:$IMAGE_TAG" \
  "$PROJECT_DIRECTORY"
