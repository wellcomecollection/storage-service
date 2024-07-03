#!/usr/bin/env bash

# Usage: ./deploy_lambda_zip.sh <project_name> <lambda_name>
# Example: ./deploy_lambda_zip.sh monitoring/ingest_inspector_backend ingest_inspector_backend

set -o errexit
set -o nounset
set -o pipefail

PROJECT_NAME=$1
LAMBDA_NAME=$2

S3_BUCKET="wellcomecollection-storage-infra"
S3_KEY="lambdas/$PROJECT_NAME.zip"

echo "Identifying function: $LAMBDA_NAME"
FUNCTION_ARN=$(aws lambda get-function-configuration \
    --function-name $LAMBDA_NAME \
    --query "FunctionArn" \
    --output text)

echo "Updating function: $FUNCTION_ARN from s3://$S3_BUCKET/$S3_KEY"
REVISION_ID=$(aws lambda update-function-code \
    --function-name $LAMBDA_NAME \
    --s3-bucket $S3_BUCKET \
    --s3-key $S3_KEY \
    --query "RevisionId" \
    --output text)

echo "Revision id: $REVISION_ID"

echo "Awaiting function update"
aws lambda wait function-updated \
    --function-name $LAMBDA_NAME

echo "Done"
