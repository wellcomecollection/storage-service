#!/usr/bin/env python
# -*- encoding: utf-8

import datetime as dt
import decimal
import json

from _aws import scan_table


class DecimalEncoder(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        return super(DecimalEncoder, self).default(o)


if __name__ == "__main__":
    date_slug = dt.datetime.now().strftime("%Y-%m-%d_%H-%m")
    out_path = f"ingests__{date_slug}.json"

    with open(out_path, "w") as outfile:
        for item in scan_table(TableName="storage-ingests"):
            outfile.write(json.dumps(item, cls=DecimalEncoder) + "\n")

    print(out_path)
