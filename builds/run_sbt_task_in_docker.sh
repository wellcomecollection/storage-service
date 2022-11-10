#!/usr/bin/env bash
<<EOF
Run an sbt task inside a Docker container.

The sbt/ivy2 cache is shared with the host instance, to avoid having to
redownload packages every time you run the container.

This script is mirrored across our Scala projects.

== Motivation ==

We use sbt as our build tool, and we want to run it in CI, but actually
installing sbt is a slow process.  To speed up the process, we've
created a Docker image that has sbt pre-installed, which can be pulled
and run much faster than installing sbt from scratch.

The image is defined here:
https://github.com/wellcomecollection/platform-infrastructure/tree/main/images/dockerfiles/sbt_wrapper

== Usage ==

Pass the same arguments as you would to sbt, e.g.

    $ run_sbt_task_in_docker.sh "project ingestor" "test"

is equivalent to

    $ sbt "project ingestor" "test"

EOF

set -o errexit
set -o nounset

ECR_REGISTRY="760097843905.dkr.ecr.eu-west-1.amazonaws.com"

ROOT=$(git rev-parse --show-toplevel)

# Coursier cache location is platform-dependent
# https://get-coursier.io/docs/cache.html#default-location
LINUX_COURSIER_CACHE=".cache/coursier/v1"

if [[ $(uname) == "Darwin" ]]
then
  HOST_COURSIER_CACHE=~/Library/Caches/Coursier/v1
else
  HOST_COURSIER_CACHE=~/$LINUX_COURSIER_CACHE
fi

docker run --tty --rm \
  --volume ~/.sbt:/root/.sbt \
  --volume ~/.ivy2:/root/.ivy2 \
  --volume "$HOST_COURSIER_CACHE:/root/$LINUX_COURSIER_CACHE" \
  --volume /var/run/docker.sock:/var/run/docker.sock \
  --volume "${DOCKER_CONFIG:-$HOME/.docker}:/root/.docker" \
  --net host \
  --volume "$ROOT:$ROOT" \
  --workdir "$ROOT" \
  "$ECR_REGISTRY/wellcome/sbt_wrapper" "$@"
