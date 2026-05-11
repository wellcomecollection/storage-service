#!/bin/bash

set -o errexit
set -o nounset

# Parse command line arguments
FOLDER="$1"
TARGET_DIR="$2"
ZIP_NAME="$3"
S3_BUCKET="$4"
S3_KEY="$5"

# Change to the project folder
cd "$FOLDER"

ZIP_FILE="$TARGET_DIR/$ZIP_NAME"
S3_DESTINATION="s3://$S3_BUCKET/$S3_KEY"

echo "Uploading $ZIP_FILE to $S3_DESTINATION"
aws s3 cp "$ZIP_FILE" "$S3_DESTINATION"
