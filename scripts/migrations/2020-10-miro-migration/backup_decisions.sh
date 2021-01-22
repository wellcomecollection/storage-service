#!/usr/bin/bash

cd /data/storage-service/scripts/migrations/2020-10-miro-migration

python3 main.py save-index --index-name decisions -o
python3 main.py save-index --index-name decisions_derivatives -o

aws s3 cp _cache/index_decisions.json s3://wellcomecollection-platform-infra/miro-migration/index_decisions.json --profile platform
aws s3 cp _cache/index_decisions_derivatives.json s3://wellcomecollection-platform-infra/miro-migration/index_decisions_derivatives.json --profile platform
