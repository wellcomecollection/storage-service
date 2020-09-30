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


def prepare_slack_payload(classified_ingests, found_everything, days_to_fetch, s3_url):
    heading = (
        f"What happened in the storage service "
        f"in the last {days_to_fetch} day{'s' if days_to_fetch > 1 else ''}?"
    )

    result = {
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

        ]
    }

    if not found_everything:
        result['blocks'].append({
            "type": "section",
            "text": {
                "type": "mrkdwn",
                "text": "Elasticsearch can only return the first 10,000 results. There may be ingests missing from this report."
            }
        })

    result['blocks'].append({
        "type": "section",
        "text": {"type": "mrkdwn", "text": f"*Full report*: {s3_url}"},
    })

    return result
