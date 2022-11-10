#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

if (( $# == 2))
then
  PROJECT_NAME="$1"
  IMAGE_TAG="$2"
else
  echo "Usage: publish_image_to_ecr_public.sh <PROJECT> <IMAGE_TAG>" >&2
  exit 1
fi

ECR_REGISTRY="public.ecr.aws/y1p3h6z3"

ROOT=$(git rev-parse --show-toplevel)

if [[ "${BUILDKITE:-}" = "true" ]]
then
  # The boto3 installed by default doesn't include the "ecr-public" commands,
  # so go ahead and grab the latest version.
  pip3 install --user --upgrade boto3
fi

PASSWORD=$(python3 "$ROOT/builds/get_ecr_public_password.py")

docker login --username AWS --password "$PASSWORD" public.ecr.aws

docker tag "$PROJECT_NAME:$IMAGE_TAG" "$ECR_REGISTRY/$PROJECT_NAME:ref.$IMAGE_TAG"
docker push "$ECR_REGISTRY/$PROJECT_NAME:ref.$IMAGE_TAG"
docker rmi "$ECR_REGISTRY/$PROJECT_NAME:ref.$IMAGE_TAG"

docker tag "$PROJECT_NAME:$IMAGE_TAG" "$ECR_REGISTRY/$PROJECT_NAME:latest"
docker push "$ECR_REGISTRY/$PROJECT_NAME:latest"
docker rmi "$ECR_REGISTRY/$PROJECT_NAME:latest"
