#!/usr/bin/env bash

set -o nounset
set -o verbose

export AWS_PROFILE=mets_sync

xargs -P 20 -I '{}' \
   aws s3 sync \
    /Volumes/Shares/LIB_WDL_DDS/LIB_WDL_DDS_METS/'{}' \
    s3://wellcomecollection-assets-workingstorage/mets/'{}' \
    --storage-class STANDARD_IA \
    --exclude "*" \
    --include "*.xml" < mets_prefixes.txt

AWS_PROFILE=mets_sync aws s3 sync \
    s3://wellcomecollection-assets-workingstorage/mets \
    s3://wellcomecollection-assets-workingstorage/mets_only \
    --exclude "*_alto*"
