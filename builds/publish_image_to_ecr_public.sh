#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o verbose

PROJECT="$1"

ECR_REGISTRY="public.ecr.aws/y1p3h6z3"

CURRENT_COMMIT=$(git rev-parse HEAD)

# The AWS CLI installed by default doesn't include the "ecr-public" commands,
# so go ahead and grab the latest version, then use that.
pip3 install --user --upgrade awscli
/var/lib/buildkite-agent/.local/bin/aws help --version

if [[ "${BUILDKITE:-}" = "true" ]]
then
  # We can only push to our ECR Public repo using the storage-ci role.
  # At some point Terraform will support ECR Public Repository policies,
  # at which point we can allow platform-ci to push to these repos and
  # ditch these slightly hacky workaround.
  #
  # See https://github.com/hashicorp/terraform-provider-aws/issues/16540
  echo "" >> ~/.aws/credentials
  echo "[storage-ci-$BUILDKITE_BUILD_ID]"                   >> ~/.aws/credentials
  echo "source_profile=default"                             >> ~/.aws/credentials
  echo "role_arn=arn:aws:iam::975596993436:role/storage-ci" >> ~/.aws/credentials
  echo "region=eu-west-1"                                   >> ~/.aws/credentials

  PASSWORD=$(AWS_PROFILE="storage-ci-$BUILDKITE_BUILD_ID" /var/lib/buildkite-agent/.local/bin/aws ecr-public get-login-password --region us-east-1)
else
  PASSWORD=$(AWS_PROFILE=storage-dev /var/lib/buildkite-agent/.local/bin/aws ecr-public get-login-password --region us-east-1)
fi

docker login --username AWS --password "$PASSWORD" public.ecr.aws

docker tag "$PROJECT:$CURRENT_COMMIT" "$ECR_REGISTRY/$PROJECT:ref.$CURRENT_COMMIT"
docker push "$ECR_REGISTRY/$PROJECT:ref.$CURRENT_COMMIT"
docker rmi "$ECR_REGISTRY/$PROJECT:ref.$CURRENT_COMMIT"

docker tag "$PROJECT:$CURRENT_COMMIT" "$ECR_REGISTRY/$PROJECT:latest"
docker push "$ECR_REGISTRY/$PROJECT:latest"
docker rmi "$ECR_REGISTRY/$PROJECT:latest"
