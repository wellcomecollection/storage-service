#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

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
	--volume "$ROOT:/repo" \
	--workdir /repo \
	"$ECR_REGISTRY/hashicorp/terraform:light" fmt -recursive

docker run --tty --rm \
  --volume ~/.sbt:/root/.sbt \
  --volume ~/.ivy2:/root/.ivy2 \
  --volume "$HOST_COURSIER_CACHE:/root/$LINUX_COURSIER_CACHE" \
	--volume "$ROOT:/repo" \
	"$ECR_REGISTRY/wellcome/scalafmt:edge"

docker run --tty --rm \
	--volume "$ROOT:/repo" \
	"$ECR_REGISTRY/wellcome/format_python:112"
