#!/usr/bin/env python

import collections
import json
import sys
import webbrowser

import httpx

from aws import get_secret
from elasticsearch import get_es_client, get_interesting_ingests
from html_report import store_s3_report
from ingests import get_dev_status
from slack import prepare_slack_payload


def main(*args):
    es_client = get_es_client()

    days_to_fetch = 2

    prod_ingests = get_interesting_ingests(
        es_client, index_name="storage_ingests", days_to_fetch=days_to_fetch
    )

    staging_ingests = get_interesting_ingests(
        es_client, index_name="storage_stage_ingests", days_to_fetch=days_to_fetch
    )

    ingests_by_status = {
        "prod": collections.defaultdict(list),
        "staging": collections.defaultdict(list),
    }

    for ingest in prod_ingests["ingests"]:
        status = get_dev_status(ingest)
        ingests_by_status["prod"][status].append(ingest)

    for ingest in staging_ingests["ingests"]:
        status = get_dev_status(ingest)
        ingests_by_status["staging"][status].append(ingest)

    found_everything = (
        prod_ingests["found_everything"] and staging_ingests["found_everything"]
    )

    s3_url = store_s3_report(
        ingests_by_status=ingests_by_status,
        found_everything=found_everything,
        days_to_fetch=days_to_fetch,
    )

    if "--browser" in sys.argv:
        webbrowser.open(s3_url)

    if "--skip-slack" not in sys.argv:
        payload = prepare_slack_payload(
            classified_ingests=classified_ingests,
            found_everything=found_everything,
            days_to_fetch=days_to_fetch,
            s3_url=s3_url,
        )

        resp = httpx.post(
            get_secret("storage_service_reporter/slack_webhook"), json=payload
        )

        print(f"Sent payload to Slack: {resp}")

        if resp.status_code != 200:
            print("Non-200 response from Slack:")

            print("")

            print("== request ==")
            print(json.dumps(payload, indent=2, sort_keys=True))

            print("")

            print("== response ==")
            print(resp.text)


if __name__ == "__main__":
    main()
