#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

ECR_REGISTRY="760097843905.dkr.ecr.eu-west-1.amazonaws.com"

ROOT=$(git rev-parse --show-toplevel)

docker run --tty --rm \
	--volume "$ROOT:/data" \
	--workdir /data \
	"$ECR_REGISTRY/wellcome/flake8:latest" \
    --exclude .git,__pycache__,target,.terraform \
    --ignore=E501,E122,E126,E203,W503
