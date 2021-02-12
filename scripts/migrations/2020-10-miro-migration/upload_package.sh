#!/usr/bin/bash

EBS_ALARM_STATE=$(aws cloudwatch describe-alarms --alarm-names ArchivematicaEbsLowSpace --profile workflow | jq ".MetricAlarms[0].StateValue" -r)

cd /data/storage-service/scripts/migrations/2020-10-miro-migration

if [ "$EBS_ALARM_STATE" = "OK" ]; then
	python3 ./main.py upload-transfer-packages --limit 2500 --index-name chunks_derivatives 
fi
