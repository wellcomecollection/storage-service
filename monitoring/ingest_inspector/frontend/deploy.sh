#!/bin/bash

npm run build
AWS_PROFILE=storage-developer aws s3 cp out s3://wellcomecollection-ingest-inspector-frontend --recursive
echo "Success"
