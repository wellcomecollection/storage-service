# -*- encoding: utf-8

import datetime as dt

from boto3.dynamodb.types import TypeDeserializer

from check_names import ALL_CHECK_NAMES


class DynamoStatusReader:
    def __init__(self, table_name="storage-migration-status"):
        from aws_client import read_only_client

        self.dynamo_client = read_only_client.dynamo_client()
        self.dynamo_table = read_only_client.dynamo_table(table_name)
        self.table_name = table_name

    @staticmethod
    def _chunks(data, rows=100):
        for i in range(0, len(data), rows):
            yield data[i : i + rows]

    @staticmethod
    def _deserialize_row(row):
        deserializer = TypeDeserializer()
        return {k: deserializer.deserialize(v) for k, v in row.items()}

    @staticmethod
    def _extract_statuses(row):
        status = {}
        status["bnumber"] = row["bnumber"]

        known_statuses = {k: v for k, v in row.items() if k.startswith("status-")}

        for status_name in ALL_CHECK_NAMES:
            if f"status-{status_name}" in known_statuses:
                success = known_statuses[f"status-{status_name}"]["success"]
                last_modified = known_statuses[f"status-{status_name}"]["last_modified"]

                status[status_name] = known_statuses[f"status-{status_name}"]
            else:
                status[status_name] = {}

        return status

    def _generate_rows(self, **kwargs):
        paginator = self.dynamo_client.get_paginator("scan")
        all_pages = paginator.paginate(**kwargs)

        for page in all_pages:
            for row in page["Items"]:
                yield self._deserialize_row(row)

    def all(self, first_bnumber=None):
        kwargs = {"TableName": self.table_name}

        if first_bnumber:
            kwargs["ExclusiveStartKey"] = {"bnumber": {"S": first_bnumber}}

        for row in self._generate_rows(**kwargs):
            yield self._extract_statuses(row)

    def get(self, bnumbers):
        for bnumbers_chunk in self._chunks(bnumbers):
            keys = [{"bnumber": {"S": bnumber}} for bnumber in bnumbers_chunk]

            response = self.dynamo_client.batch_get_item(
                RequestItems={self.table_name: {"Keys": keys}}
            )

            rows = response["Responses"]["storage-migration-status"]
            deserialized_rows = [self._deserialize_row(row) for row in rows]
            status_rows = [self._extract_statuses(row) for row in deserialized_rows]

            for row in status_rows:
                yield row


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

    def reset(self, bnumber):
        response = self.dynamo_table.get_item(Key={"bnumber": bnumber})

        if "Item" in response:
            item = response["Item"]

            for status_name in ALL_CHECK_NAMES:

                item[f"status-{status_name}"] = {
                    "success": False,
                    "last_modified": dt.datetime.now().isoformat(),
                }

            self._put_item(item=item)
        else:
            print(f"No Item in response: {response}!")

    def update(self, item, *, status_name, success, last_modified=None):
        if not status_name in ALL_CHECK_NAMES:
            raise Exception(
                f"{status_name} is not valid (should be one of {ALL_CHECK_NAMES})."
            )

        item[f"status-{status_name}"] = {
            "success": success,
            "last_modified": last_modified or dt.datetime.now().isoformat(),
        }

        self._put_item(item=item)
