# -*- encoding: utf-8

import datetime as dt

from boto3.dynamodb.types import TypeDeserializer


class DynamoStatusReader:
    def __init__(self, table_name="storage-migration-status"):
        from aws_client import dev_client

        self.dynamo_client = dev_client.dynamo_client()
        self.dynamo_table = dev_client.dynamo_table(table_name)
        self.table_name = table_name

    @staticmethod
    def _deserialize_row(row):
        deserializer = TypeDeserializer()
        return {k: deserializer.deserialize(v) for k, v in row.items()}

    def _generate_rows(self, **kwargs):
        paginator = self.dynamo_client.get_paginator("scan")
        all_pages = paginator.paginate(**kwargs)

        for page in all_pages:
            for row in page["Items"]:
                yield self._deserialize_row(row)

    def get_all_statuses(self, first_bnumber=None):
        kwargs = {
            "TableName": self.table_name,
        }

        if first_bnumber:
            kwargs["ExclusiveStartKey"] = {"bnumber": {"S": first_bnumber}}

        for row in self._generate_rows(**kwargs):
            yield row

    def get_status(self, bnumber):
        resp = self.dynamo_table.get_item(Key={"bnumber": bnumber})
        row = resp["Item"]
        return {k: v for k, v in row.items() if k.startswith("status-")}


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
