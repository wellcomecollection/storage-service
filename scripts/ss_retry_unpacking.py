#!/usr/bin/env python
"""
Given an ingest ID, retry the ingest of a bag *under the same ingest ID*.

This works by sending a message to the bag unpacker, and letting the rest of
the ingest flow through the pipeline as normal.

I prefer sending it through the unpacker to trying to restart at a particular
point in the pipeline -- it reduces the risk of sending the wrong sort of message,
or inadvertently skipping an important check.  The pipeline should be idempotent,
so redoing an entire ingest shouldn't be an issue.
"""

import json
import sys
import webbrowser

from _aws import get_aws_client, DEV_ROLE_ARN
from ss_get_ingest import lookup_ingest


def create_context(ingest):
    return {
        "ingestId": ingest["id"],
        "ingestType": {"id": ingest["ingestType"]["id"]},
        "storageSpace": ingest["space"]["id"],
        "externalIdentifier": ingest["bag"]["info"]["externalIdentifier"],
        "ingestDate": ingest["createdDate"],
    }


if __name__ == "__main__":
    try:
        ingest_id = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <INGEST_ID>")

    name, ingest = lookup_ingest(ingest_id)

    context = create_context(ingest)

    payload = {
        "context": context,
        "sourceLocation": {
            "location": {
                "bucket": ingest["sourceLocation"]["bucket"],
                "key": ingest["sourceLocation"]["path"],
            },
            "type": "S3SourceLocation",
        },
        "type": "SourceLocationPayload",
    }

    if name == "prod":
        topic_arn = "arn:aws:sns:eu-west-1:975596993436:storage-prod_bag_unpacker_input"
    elif name == "stage":
        topic_arn = (
            "arn:aws:sns:eu-west-1:975596993436:storage-staging_bag_unpacker_input"
        )
    else:
        assert False, f"Unrecognised API name: {name}"

    sns = get_aws_client("sns", role_arn=DEV_ROLE_ARN)

    resp = sns.publish(TopicArn=topic_arn, Message=json.dumps(payload))
    print(resp)

    webbrowser.open(f"https://wellcome-ingest-inspector.glitch.me/ingests/{ingest_id}")
