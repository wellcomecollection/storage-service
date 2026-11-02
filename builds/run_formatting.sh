#!/usr/bin/env bash

set -o errexit
set -o nounset

ROOT=$(git rev-parse --show-toplevel)

docker run --tty --rm \
	--volume "$ROOT:/repo" \
	--workdir /repo \
	"public.ecr.aws/hashicorp/terraform:light" fmt -recursive

./builds/run_sbt_task_in_docker.sh "scalafmt"

docker run --tty --rm \
	--volume "$ROOT:/repo" \
  --workdir /repo \
	"ghcr.io/astral-sh/ruff:0.16.1" \
  format --extend-exclude ".lambda_zips,.terraform,target,*.ipynb,*.md" .
