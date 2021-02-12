#!/usr/bin/bash

cd /data/storage-service/scripts/migrations/2020-10-miro-migration

python3 main.py create-files-index
python3 main.py create-registrations-index --location derivative

python3 main.py dlcs-update-registrations --location derivative

python3 main.py save-index --index-name files -o
python3 main.py save-index --index-name registrations_derivatives -o 

#aws s3 cp registration_clearup/ambiguous_decisions.json s3://wellcomecollection-platform-infra/miro-migration/ambiguous_decisions.json --profile platform 
#aws s3 cp registration_clearup/files_for_registration.json s3://wellcomecollection-platform-infra/miro-migration/files_for_registration.json --profile platform
#aws s3 cp registration_clearup/missing_miro_ids.json s3://wellcomecollection-platform-infra/miro-migration/missing_miro_ids.json --profile platform

aws s3 cp _cache/index_files.json s3://wellcomecollection-platform-infra/miro-migration/index_files.json --profile platform
aws s3 cp _cache/index_registrations_derivatives.json s3://wellcomecollection-platform-infra/miro-migration/index_registrations_derivatives.json --profile platform
