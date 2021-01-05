#!/bin/bash

aws s3 cp s3://wellcomecollection-platform-infra/miro-migration/index_chunks.json _cache/index_chunks.json --profile platform
aws s3 cp s3://wellcomecollection-platform-infra/miro-migration/index_chunks_movies_and_corporate.json _cache/index_chunks_movies_and_corporate.json --profile platform
aws s3 cp s3://wellcomecollection-platform-infra/miro-migration/index_chunks_no_miro_id.json _cache/index_chunks_no_miro_id.json --profile platform
aws s3 cp s3://wellcomecollection-platform-infra/miro-migration/index_transfers.json _cache/index_transfers.json --profile platform

python3 main.py load-index --index-name chunks -o
python3 main.py load-index --index-name chunks_movies_and_corporate -o
python3 main.py load-index --index-name chunks_no_miro_id -o
python3 main.py load-index --index-name transfers -o
