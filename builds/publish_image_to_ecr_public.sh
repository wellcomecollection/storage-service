#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

PROJECT="$1"

ECR_REGISTRY="public.ecr.aws/y1p3h6z3"

CURRENT_COMMIT=$(git rev-parse HEAD)
ROOT=$(git rev-parse --show-toplevel)

if [[ "${BUILDKITE:-}" = "true" ]]
then
  # The boto3 installed by default doesn't include the "ecr-public" commands,
  # so go ahead and grab the latest version.
  pip3 install --user --upgrade boto3
fi

PASSWORD=$(python3 "$ROOT/builds/get_ecr_public_password.py")

docker login --username AWS --password "$PASSWORD" public.ecr.aws

docker tag "$PROJECT:$CURRENT_COMMIT" "$ECR_REGISTRY/$PROJECT:ref.$CURRENT_COMMIT"
docker push "$ECR_REGISTRY/$PROJECT:ref.$CURRENT_COMMIT"
docker rmi "$ECR_REGISTRY/$PROJECT:ref.$CURRENT_COMMIT"

docker tag "$PROJECT:$CURRENT_COMMIT" "$ECR_REGISTRY/$PROJECT:latest"
docker push "$ECR_REGISTRY/$PROJECT:latest"
docker rmi "$ECR_REGISTRY/$PROJECT:latest"
