#!/usr/bin/env python

import collections
import datetime

from elasticsearch import get_es_client, get_interesting_ingests
from html_report import store_s3_report
from ingests import classify_ingest


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
    elif len(recent_ingests) == 0:
        summary = "No activity"
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

    payload = {"blocks": [summary_block]}

    if recent_ingests:
        if len(ingests_by_status["succeeded"]):
            status_block = {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": "All succeeded"}],
            }
        else:
            status_block = {
                "type": "context",
                "elements": [{"type": "mrkdwn", "text": " / ".join(pretty_statuses)}],
            }

        payload["blocks"].append(status_block)

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
    es_client = get_es_client()

    days_to_fetch = 2

    prod_ingests = get_interesting_ingests(
        es_client, index_name="storage_ingests", days_to_fetch=days_to_fetch
    )

    staging_ingests = get_interesting_ingests(
        es_client, index_name="storage_stage_ingests", days_to_fetch=days_to_fetch
    )

    classified_ingests = {
        'prod': collections.defaultdict(list),
        'staging': collections.defaultdict(list),
    }

    for ingest in prod_ingests["ingests"]:
        status = classify_ingest(ingest)
        classified_ingests['prod'][status].append(ingest)

    for ingest in staging_ingests["ingests"]:
        status = classify_ingest(ingest)
        classified_ingests['staging'][status].append(ingest)

    found_everything = (
        prod_ingests["found_everything"] and staging_ingests["found_everything"]
    )

    s3_url = store_s3_report(
        classified_ingests=classified_ingests,
        found_everything=found_everything,
        days_to_fetch=days_to_fetch
    )

    print(s3_url)

    #
    #
    # prod_payload = prepare_slack_payload(
    #     prod_ingests, name="the storage service", time_period="48 hours"
    # )
    # stage_payload = prepare_slack_payload(
    #     stage_ingests, name="the staging service", time_period="48 hours"
    # )
    #
    # complete_payload = {
    #     "blocks": prod_payload["blocks"]
    #     + [{"type": "divider"}]
    #     + stage_payload["blocks"]
    # }
    #
    # resp = httpx.post(
    #     get_secret("storage_service_reporter/slack_webhook"), json=complete_payload
    # )
    #
    # print(f"Sent payload to Slack: {resp}")
    #
    # if resp.status_code != 200:
    #     print("Non-200 response from Slack:")
    #
    #     print("")
    #
    #     print("== request ==")
    #     print(json.dumps(complete_payload, indent=2, sort_keys=True))
    #
    #     print("")
    #
    #     print("== response ==")
    #     print(resp.text)


if __name__ == "__main__":
    main()
