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

    def get_all_table_rows(self):
        paginator = self.dynamo_client.get_paginator("scan")
        all_pages = paginator.paginate(TableName=self.table_name)

        for page in all_pages:
            for row in page["Items"]:
                yield self._deserialize_row(row)

    def update_status(self, bnumber, *, status_name, success, last_modified=None):
        resp = self.dynamo_table.get_item(Key={"bnumber": bnumber})
        item = resp["Item"]

        item[f"status-{status_name}"] = {
            "success": success,
            "last_modified": last_modified or dt.datetime.now().isoformat(),
        }

        self.dynamo_table.put_item(Item=item)
