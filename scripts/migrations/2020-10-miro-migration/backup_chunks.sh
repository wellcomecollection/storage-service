#!/usr/bin/bash

cd /data/storage-service/scripts/migrations/2020-10-miro-migration

python3 main.py create-files-index
python3 main.py save-index --index-name files

python3 main.py save-index --index-name chunks -o
python3 main.py save-index --index-name chunks_movies_and_corporate -o
python3 main.py save-index --index-name chunks_no_miro_id -o
python3 main.py save-index --index-name transfers -o

aws s3 cp _cache/index_chunks.json s3://wellcomecollection-platform-infra/miro-migration/index_chunks.json --profile platform
aws s3 cp _cache/index_chunks_movies_and_corporate.json s3://wellcomecollection-platform-infra/miro-migration/index_chunks_movies_and_corporate.json --profile platform
aws s3 cp _cache/index_chunks_no_miro_id.json s3://wellcomecollection-platform-infra/miro-migration/index_chunks_no_miro_id.json --profile platform
aws s3 cp _cache/index_transfers.json s3://wellcomecollection-platform-infra/miro-migration/index_transfers.json --profile platform

aws s3 cp _cache/index_files.json s3://wellcomecollection-platform-infra/miro-migration/index_files.json --profile platform
