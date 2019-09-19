#!/usr/bin/env python
# -*- encoding: utf-8

import datetime as dt
import decimal
import json

from common import get_read_only_aws_resource


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


if __name__ == "__main__":
    dynamodb = get_read_only_aws_resource("dynamodb").meta.client
    paginator = dynamodb.get_paginator("scan")

    date_slug = dt.datetime.now().strftime("%Y-%m-%d_%H-%M")
    out_path = f"ingests__{date_slug}.json"

    with open(out_path, "w") as outfile:
        for page in paginator.paginate(TableName="storage-ingests"):
            for item in page["Items"]:
                outfile.write(json.dumps(item, cls=DecimalEncoder) + "\n")
            outfile.flush()

    print(out_path)
