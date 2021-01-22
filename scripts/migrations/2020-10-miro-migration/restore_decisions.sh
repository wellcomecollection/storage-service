#!/bin/bash

aws s3 cp s3://wellcomecollection-platform-infra/miro-migration/index_decisions.json _cache/index_decisions.json --profile platform
aws s3 cp s3://wellcomecollection-platform-infra/miro-migration/index_decisions_derivatives.json _cache/index_decisions_derivatives.json --profile platform

python3 main.py load-index --index-name decisions -o
python3 main.py load-index --index-name decisions_derivatives -o
