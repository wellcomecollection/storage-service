#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

ECR_REGISTRY="760097843905.dkr.ecr.eu-west-1.amazonaws.com"
WECO_DEPLOY_IMAGE="wellcome/weco-deploy:5.6.11"

ROOT=$(git rev-parse --show-toplevel)

PROJECT_ID="$1"
IMAGE_ID="$2"

docker run --tty --rm \
  --env AWS_PROFILE \
  --volume ~/.aws:/root/.aws \
  --volume /var/run/docker.sock:/var/run/docker.sock \
  --volume ~/.docker:/root/.docker \
  --volume "$ROOT:$ROOT" \
  --workdir "$ROOT" \
  "$ECR_REGISTRY/$WECO_DEPLOY_IMAGE" \
    --project-id="$PROJECT_ID" \
    --verbose \
    publish \
    --image-id="$IMAGE_ID"
