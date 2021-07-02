#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

PROJECT="$1"

ECR_REGISTRY="public.ecr.aws/y1p3h6z3"

CURRENT_COMMIT=$(git rev-parse HEAD)

if [[ "${BUILDKITE:-}" = "true" ]]
then
  # The AWS CLI installed by default doesn't include the "ecr-public" commands,
  # so go ahead and grab the latest version, then use that.
  pip3 install --user --upgrade awscli
  /var/lib/buildkite-agent/.local/bin/aws help --version

  PASSWORD=$(AWS_PROFILE=platform-ci /var/lib/buildkite-agent/.local/bin/aws ecr-public get-login-password --region us-east-1)
else
  PASSWORD=$(AWS_PROFILE=storage-dev aws ecr-public get-login-password --region us-east-1)
fi

docker login --username AWS --password "$PASSWORD" public.ecr.aws

docker tag "$PROJECT:$CURRENT_COMMIT" "$ECR_REGISTRY/$PROJECT:ref.$CURRENT_COMMIT"
docker push "$ECR_REGISTRY/$PROJECT:ref.$CURRENT_COMMIT"
docker rmi "$ECR_REGISTRY/$PROJECT:ref.$CURRENT_COMMIT"

docker tag "$PROJECT:$CURRENT_COMMIT" "$ECR_REGISTRY/$PROJECT:latest"
docker push "$ECR_REGISTRY/$PROJECT:latest"
docker rmi "$ECR_REGISTRY/$PROJECT:latest"
