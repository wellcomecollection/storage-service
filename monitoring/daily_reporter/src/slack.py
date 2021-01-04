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
            status_descriptions.append(f"*{len(individual_ingests)} {status}*")
        else:
            status_descriptions.append(f"{len(individual_ingests)} {status}")

    result += ", ".join(status_descriptions)

    # Create a list of any ingests that are likely to need special attention.
    # The list is of the form
    #
    #       - a123-a123-a123 – digitised/b1234/v1
    #
    # and the ingest UUID links to the ingest inspector.
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


def prepare_slack_payload(ingests_by_status, found_everything, days_to_fetch, s3_url):
    # Are there any interesting failures that would be worth alerting on?
    interesting_failures = 0

    for env in ("prod", "staging"):
        for status in ("failed (unknown reason)", "stalled"):
            interesting_failures += len(ingests_by_status[env].get(status, []))

    # And use this to customise the heading
    emoji = ":white_check_mark:" if interesting_failures == 0 else ":interrobang:"

    heading = f"{emoji} What happened in the storage service recently?"

    result = {
        "blocks": [
            {"type": "header", "text": {"type": "plain_text", "text": heading}},
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": _get_slack_message(
                        "Prod", ingests=ingests_by_status["prod"]
                    ),
                },
            },
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": _get_slack_message(
                        "Staging", ingests=ingests_by_status["staging"]
                    ),
                },
            },
        ]
    }

    if not found_everything:
        result["blocks"].append(
            {
                "type": "section",
                "text": {
                    "type": "mrkdwn",
                    "text": "Elasticsearch can only return the first 10,000 results. There may be ingests missing from this report.",
                },
            }
        )

    result["blocks"].append(
        {
            "type": "section",
            "text": {"type": "mrkdwn", "text": f"*Full report*: {s3_url}"},
        }
    )

    return result
