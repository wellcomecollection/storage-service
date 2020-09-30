#!/usr/bin/env python

import collections
import json

import httpx

from aws import get_secret
from elasticsearch import get_es_client, get_interesting_ingests
from html_report import store_s3_report
from ingests import classify_ingest


def _get_slack_message(label, ingests):
    result = f"*{label}:* "

    if not ingests:
        result += "no activity."
        return result

    # Produce a message of the form
    #
    #     Prod: succeeded (14), failed (user error) (2), stalled (1)
    #
    # Highlight unusual statuses.
    status_descriptions = []
    for status, individual_ingests in ingests.items():
        if status in ("stalled", "failed (unknown reason)"):
            status_descriptions.append(f"*{status} ({len(individual_ingests)})*")
        else:
            status_descriptions.append(f"{status} ({len(individual_ingests)})")

    result += ", ".join(status_descriptions)

    for status in ("failed (unknown reason)", "stalled"):
        if ingests.get(status, []):
            result += "\n" + status.title() + ":"
            for i in ingests[status][:15]:
                result += f"\n- <https://wellcome-ingest-inspector.glitch.me/ingests/{i['id']}|`{i['id']}`> – {i['space']}/{i['externalIdentifier']}"
                if i["version"]:
                    result += "/" + i["version"]

            if len(ingests[status]) >= 15:
                result += "\n- ..."

    return result


def prepare_slack_payload(classified_ingests, found_everything, days_to_fetch, s3_url):
    heading = (
        f"What happened in the storage service "
        f"in the last {days_to_fetch} day{'s' if days_to_fetch > 1 else ''}?"
    )

    return {
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": heading}},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": _get_slack_message(
                        "Prod", ingests=classified_ingests["prod"]
                    ),
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": _get_slack_message(
                        "Staging", ingests=classified_ingests["staging"]
                    ),
                },
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"*Full report*: {s3_url}"},
            },
        ]
    }


def main(*args):
    es_client = get_es_client()

    days_to_fetch = 2

    prod_ingests = get_interesting_ingests(
        es_client, index_name="storage_ingests", days_to_fetch=days_to_fetch
    )

    staging_ingests = get_interesting_ingests(
        es_client, index_name="storage_stage_ingests", days_to_fetch=days_to_fetch
    )

    classified_ingests = {
        "prod": collections.defaultdict(list),
        "staging": collections.defaultdict(list),
    }

    for ingest in prod_ingests["ingests"]:
        status = classify_ingest(ingest)
        classified_ingests["prod"][status].append(ingest)

    for ingest in staging_ingests["ingests"]:
        status = classify_ingest(ingest)
        classified_ingests["staging"][status].append(ingest)

    found_everything = (
        prod_ingests["found_everything"] and staging_ingests["found_everything"]
    )

    s3_url = store_s3_report(
        classified_ingests=classified_ingests,
        found_everything=found_everything,
        days_to_fetch=days_to_fetch,
    )

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
