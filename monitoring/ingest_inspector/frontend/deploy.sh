#!/bin/sh

echo "Building app..."
npm run build

# Exit without uploading if build failed
if [ $? -ne 0 ]; then
  exit 1
fi

echo "Uploading to S3..."
AWS_PROFILE=storage-developer aws s3 cp out s3://wellcomecollection-ingest-inspector-frontend --recursive --only-show-errors

if [ $? -eq 0 ]; then
  echo "Success!"
else
  echo "Something went wrong while uploading to S3. See error message above."
  exit 1
fi
