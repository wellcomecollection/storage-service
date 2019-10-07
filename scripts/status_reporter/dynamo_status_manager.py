from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.conditions import Attr

import datetime as dt


class DynamoStatusManager:
    def __init__(self, dev_client, table_name="storage-migration-status"):
        self.table_name = table_name

        self.dynamo_client = dev_client.dynamo_client()
        self.dynamo_table = dev_client.dynamo_table(self.table_name)

    @staticmethod
    def _deserialize_row(row):
        deserializer = TypeDeserializer()
        return {k: deserializer.deserialize(v) for k, v in row.items()}

    def _row_scan_generator(self, *args, **kwargs):
        paginator = self.dynamo_client.get_paginator("scan")
        all_pages = paginator.paginate(*args, **kwargs)

        for page in all_pages:
            for row in page["Items"]:
                yield self._deserialize_row(row)

    def _get_raw_row(self, bnumber):
        resp = self.dynamo_table.get_item(Key={"bnumber": bnumber})
        return resp["Item"]

    def get_row_status(self, bnumber):
        row = self._get_raw_row(bnumber)
        return {k: v for k, v in row.items() if k.startswith("status-")}

    def get_all_table_rows(self):
        return self._row_scan_generator(TableName=self.table_name)

    def get_all_with_status(self, status_name):
        return self._row_scan_generator(
            TableName=self.table_name,
            ScanFilter={
                status_name: {
                    'ComparisonOperator': 'NOT_NULL'
                }
            })

    def get_all_without_status(self, status_name):
        return self._row_scan_generator(
            TableName=self.table_name,
            ScanFilter={
                status_name: {
                    'ComparisonOperator': 'NULL'
                }
            })

    def update_status(self, bnumber, *, status_name, success, last_modified=None):
        item = self._get_raw_row(bnumber)

        item[f"status-{status_name}"] = {
            "success": success,
            "last_modified": last_modified or dt.datetime.now().isoformat(),
        }

        self.dynamo_table.put_item(Item=item)
