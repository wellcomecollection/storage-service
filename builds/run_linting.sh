#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

ROOT=$(git rev-parse --show-toplevel)

docker run --tty --rm \
	--volume "$ROOT:/data" \
	--workdir /data \
	"ghcr.io/astral-sh/ruff:0.16.1" \
    check --select E,F,W --ignore E501 \
    --extend-exclude ".lambda_zips,.terraform,target,*.ipynb,*.md" .
