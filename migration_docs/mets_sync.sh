#!/usr/bin/env bash

set -o nounset
set -o verbose

for i in ? 0 1 2 3 4 5 6 7 8 9 a i x
do
  AWS_PROFILE=mets_sync aws s3 sync \
    /Volumes/Shares/LIB_WDL_DDS/LIB_WDL_DDS_METS/"$i" \
    s3://wellcomecollection-assets-workingstorage/mets/"$i" \
    --storage-class STANDARD_IA \
    --exclude "*" \
    --include "*.xml" \
    --delete &
done

while true
do
  ps -eaf | grep python | grep sync
  if (( $? == 1 ))
  then
    break
  done
  sleep 1
done

AWS_PROFILE=mets_sync aws s3 sync \
    s3://wellcomecollection-assets-workingstorage/mets \
    s3://wellcomecollection-assets-workingstorage/mets_only \
    --exclude "*_alto*"
