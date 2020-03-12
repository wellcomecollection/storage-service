#!/usr/bin/env python

import requests

resp = requests.post(
    "http://localhost:9001/ingests",
    json={
        "type": "Ingest",
        "ingestType": {"id": "create", "type": "IngestType"},
        "space": {"id": "alex_testing", "type": "Space"},
        "sourceLocation": {
            "type": "Location",
            "provider": {"type": "Provider", "id": "aws-s3-standard"},
            "bucket": "bukkit",
            "path": "example123.tar.gz",
        },
        "bag": {
            "type": "Bag",
            "info": {"type": "BagInfo", "externalIdentifier": "example123"},
        },
    }
)

from pprint import pprint
pprint(resp)
print(resp.headers)