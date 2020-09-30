#!/usr/bin/env python

import datetime
import json

import boto3
import httpx

from elasticsearch import get_es_client


def get_interesting_ingests(es_client, *, index_name, days_to_fetch):
    """
    Get ingests that are "interesting".
    """
    today = datetime.datetime.now()

    start = today - datetime.timedelta(days=days_to_fetch)

    # Did we find everything we were looking for?  It's possible we might
    found_everything = True

    # Ingests we want to return
    ingests = {}

    # We make three requests to Elasticsearch, to maximise the chance of finding
    # ingests that might need more attention:
    #
    #   * everything modified in the last N days
    #   * everything modified in the last N days which hasn't succeeded
    #   * everything which hasn't completed
    #
    # A single ES request returns up to 10000 documents; if more than that have
    # been updated (e.g. we've just done a big migration), we want to flag that
    # fact and try to find the most interesting stuff.
    filters = [
        [{"range": {"lastModifiedDate": {"gte": start.strftime("%Y-%m-%d")}}}],
        [{
            "range": {"lastModifiedDate": {"gte": start.strftime("%Y-%m-%d")}}},{
            "terms": {"status.id": ["processing", "accepted", "failed"]},
        }],
        [{"terms": {"status.id": ["processing", "accepted"]}}],
    ]

    for filter_clause in filters:
        resp = es_client.request(
            method="GET",
            url=f"/{index_name}/_search",
            json={
                "query": {"bool": {"filter": filter_clause}},
                "_source": [
                    "id",
                    "lastModifiedDate",
                    "space.id",
                    "status.id",
                    "bag.info.externalIdentifier",
                    "bag.info.version",
                    "createdDate",
                    "events.description",
                ],
                # We can retrieve up to 10000 documents in one request.
                "size": 10000,
                "sort": [{"id": "desc"}],
            },
        )

        # We use a dict to de-duplicate result.  The same ingest may appear
        # multiple times in this response -- that's not an issue.
        for hit in resp.json()["hits"]["hits"]:
            ingests[hit["_id"]] = _parse_ingest_from_hit(hit)

        # If there are more than 10000 results, that suggests there are more
        # that we didn't get.  Flag this to the caller.
        if resp.json()["hits"]["total"]["value"] >= 10000:
            found_everything = False

    return {"ingests": ingests, "found_everything": found_everything}


def _parse_date(d):
    try:
        return datetime.datetime.strptime(d, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return datetime.datetime.strptime(d, "%Y-%m-%dT%H:%M:%SZ")


def _parse_ingest_from_hit(hit):
    source = hit["_source"]
    ingest = {
        "id": source["id"],
        "space": source["space"]["id"],
        "status": source["status"]["id"],
        "externalIdentifier": source["bag"]["info"]["externalIdentifier"],
        "version": source["bag"].get("version"),
        "createdDate": _parse_date(source["lastModifiedDate"]),
        "events": [ev["description"] for ev in source["events"]]
    }

    try:
        ingest["lastModifiedDate"] = _parse_date(source["lastModifiedDate"])
    except KeyError:
        ingest["lastModifiedDate"] = ingest["createdDate"]

    return ingest


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

    prod_ingests = get_interesting_ingests(
        es_client, index_name="storage_ingests", days_to_fetch=2
    )
    # stage_ingests = get_recent_ingests(
    #     es_client, index_name="storage_stage_ingests", days_to_fetch=2
    # )

    from pprint import pprint

    pprint(prod_ingests["ingests"])

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
