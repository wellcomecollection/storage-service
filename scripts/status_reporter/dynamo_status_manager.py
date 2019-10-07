# -*- encoding: utf-8

from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.conditions import Attr


def get_all_statuses(table_name="storage-migration-status", first_bnumber=None):
    from aws_client import read_only_client

    deserializer = TypeDeserializer()

    dynamo_client = read_only_client.dynamo_client()

    pagination_kwargs = {"TableName": table_name}

    if first_bnumber:
        pagination_kwargs["ExclusiveStartKey"] = {"bnumber": {"S": first_bnumber}}

    paginator = dynamo_client.get_paginator("scan")
    all_pages = paginator.paginate(**pagination_kwargs)

    for page in all_pages:
        for row in page["Items"]:
            yield {key: deserializer.deserialize(value) for key, value in row.items()}


class DynamoStatusUpdater:
    def __enter__(self, table_name="storage-migration-status"):
        from aws_client import dev_client

        self.dynamo_table = dev_client.dynamo_table(table_name)

        self._put_cache = []

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Drain any remaining items from the cache before returning.
        with self.dynamo_table.batch_writer() as batch:
            for item in self._put_cache:
                batch.put_item(Item=item)

    def _put_item(self, item):
        # Rather than putting items one-at-a-time, we hold a buffer of items
        # internally, and when it gets big enough, we pause to do a batch write
        # to the table.
        self._put_cache.append(item)

        if len(self._put_cache) < 100:
            return

        with self.dynamo_table.batch_writer() as batch:
            for item in self._put_cache:
                batch.put_item(Item=item)

        self._put_cache = []

    def update_status(self, bnumber, *, status_name, success, last_modified=None):
        item = self.dynamo_table.get_item(Key={"bnumber": bnumber})["Item"]

        item[f"status-{status_name}"] = {
            "success": success,
            "last_modified": last_modified or dt.datetime.now().isoformat(),
        }

        self._put_item(item=item)


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
            ScanFilter={status_name: {"ComparisonOperator": "NOT_NULL"}},
        )

    def get_all_without_status(self, status_name):
        return self._row_scan_generator(
            TableName=self.table_name,
            ScanFilter={status_name: {"ComparisonOperator": "NULL"}},
        )
