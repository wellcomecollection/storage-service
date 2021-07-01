#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

PROJECT="$1"

ECR_REGISTRY="public.ecr.aws/y1p3h6z3"

CURRENT_COMMIT=$(git rev-parse HEAD)

if [[ "${BUILDKITE:-}" = "true" ]]
then
  AWS_PROFILE=""
else
  AWS_PROFILE="storage-dev"
fi

AWS_PROFILE="$AWS_PROFILE" \
  aws ecr-public get-login-password --region us-east-1 | \
  docker login --username AWS --password-stdin public.ecr.aws

docker tag "$PROJECT:$CURRENT_COMMIT" "$ECR_REGISTRY/$PROJECT:ref.$CURRENT_COMMIT"
docker push "$ECR_REGISTRY/$PROJECT:ref.$CURRENT_COMMIT"
docker rmi "$ECR_REGISTRY/$PROJECT:ref.$CURRENT_COMMIT"

docker tag "$PROJECT:$CURRENT_COMMIT" "$ECR_REGISTRY/$PROJECT:latest"
docker push "$ECR_REGISTRY/$PROJECT:latest"
docker rmi "$ECR_REGISTRY/$PROJECT:latest"
