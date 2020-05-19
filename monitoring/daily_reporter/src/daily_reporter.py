#!/usr/bin/env python

import collections
import datetime

import boto3
import httpx


secrets_client = boto3.client("secretsmanager")


def get_secret(secret_id):
    return secrets_client.get_secret_value(SecretId=secret_id)["SecretString"]


def get_recent_ingests(es_client, *, index_name, days_to_fetch):
    """
    Get ingests that were modified in the last N days.
    """
    today = datetime.datetime.now()

    start = today - datetime.timedelta(days=days_to_fetch)

    resp = es_client.request(
        method="GET",
        url=f"/{index_name}/_search",
        json={
            "query": {
                "bool": {
                    "filter": [
                        {
                            "range": {
                                "lastModifiedDate": {"gte": start.strftime("%Y-%m-%d")}
                            }
                        }
                    ]
                }
            },
            "size": 1000,
        },
    )

    return [hit["_source"] for hit in resp.json()["hits"]["hits"]]


def get_ingests_by_status(recent_ingests):
    ingests_by_status = {
        "succeeded": [],
        "accepted": [],
        "processing": [],
        "failed (user error)": [],
        "failed (unknown reason)": [],
    }

    for ingest in recent_ingests:
        status = ingest["status"]["id"]

        # We sort failures into two groups:
        #
        #   -   a user error is one that means there was something wrong with the
        #       bag, e.g. it couldn't be unpacked correctly, it failed verification
        #   -   an unknown error is one that we can't categorise, and might indicate
        #       a storage service error, e.g. a replication failure
        #
        if status == "failed":
            failure_reasons = [
                event["description"]
                for event in ingest["events"]
                if "failed" in event["description"]
            ]

            if failure_reasons and all(
                reason.startswith(
                    (
                        "Verification (pre-replicating to archive storage) failed",
                        "Unpacking failed",
                        "Assigning bag version failed",
                    )
                )
                for reason in failure_reasons
            ):
                status = "failed (user error)"
            else:
                status = "failed (unknown reason)"

        ingests_by_status[status].append(ingest)

    return ingests_by_status


def prepare_slack_payload(recent_ingests, name, time_period):
    # Create a top-level summary, e.g.
    #
    #     5 ingests were updated in the storage service in the last 2 days.
    #
    if len(recent_ingests) == 1:
        summary = "1 ingest was updated"
    else:
        summary = f"{len(recent_ingests)} ingests were updated"

    summary += f" in {name} in the last {time_period}."

    summary_block = {
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*{summary}*"},
    }

    # Produce a block that summarises what state those ingests were in.
    ingests_by_status = get_ingests_by_status(recent_ingests)

    pretty_statuses = [
        f"{len(ingests)} {status}"
        for status, ingests in ingests_by_status.items()
        if ingests
    ]

    status_block = {
        "type": "context",
        "elements": [{"type": "mrkdwn", "text": " / ".join(pretty_statuses)}],
    }

    payload = {"blocks": [summary_block, status_block]}

    # If there were any ingests with unknown failures, we should highlight them!
    #
    # Create a bulleted list of ingest IDs that had unrecognised failures, a link
    # to the ingest inspector, and a hint as to why they failed.
    #
    unknown_failures = ingests_by_status["failed (unknown reason)"]
    if unknown_failures:
        if len(unknown_failures) == 1:
            message = (
                f"@here There was one failure that might be worth investigating:\n"
            )
        else:
            message = f"@here There were {len(unknown_failures)} failures that might be worth investigating:\n"

        for ingest in unknown_failures:
            ingest_id = ingest["id"]
            message += f"\n- <https://wellcome-ingest-inspector.glitch.me/ingests/{ingest_id}|`{ingest_id}`>"

            try:
                failure_description = ingest["failureDescriptions"]
                if len(failure_description) > 40:
                    failure_description = failure_description[:38] + "..."
            except KeyError:
                pass
            else:
                message += f" - {failure_description}"

        payload["blocks"].append(
            {"type": "section", "text": {"text": message, "type": "mrkdwn"}}
        )

    return payload


def main(*args):
    es_host = get_secret("storage_service_reporter/es_host")
    es_port = get_secret("storage_service_reporter/es_port")
    es_user = get_secret("storage_service_reporter/es_user")
    es_pass = get_secret("storage_service_reporter/es_pass")

    elasticsearch_url = f"https://{es_host}:{es_port}"
    with httpx.Client(base_url=elasticsearch_url, auth=(es_user, es_pass)) as es_client:
        prod_ingests = get_recent_ingests(
            es_client, index_name="storage_ingests", days_to_fetch=2
        )
        stage_ingests = get_recent_ingests(
            es_client, index_name="storage_stage_ingests", days_to_fetch=2
        )

    prod_payload = prepare_slack_payload(
        prod_ingests, name="the storage service", time_period="48 hours"
    )
    stage_payload = prepare_slack_payload(
        stage_ingests, name="the staging service", time_period="48 hours"
    )

    complete_payload = {
        "blocks": prod_payload["blocks"]
        + [{"type": "divider"}]
        + stage_payload["blocks"]
    }

    resp = httpx.post(
        get_secret("storage_service_reporter/slack_webhook"), json=complete_payload
    )
    print(f"Sent payload to Slack: {resp}")


if __name__ == "__main__":
    main()
