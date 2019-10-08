#!/usr/bin/env python
# -*- encoding: utf-8

import collections
import datetime as dt

import termcolor

from dynamodb import cached_scan_iterator


def draw_chart(data, colors=None):
    if colors is None:
        colors = {}

    max_value = max(count for _, count in data)
    increment = max_value / 25

    longest_label_length = max(len(label) for label, _ in data)

    for label, count in data:

        # The ASCII block elements come in chunks of 8, so we work out how
        # many fractions of 8 we need.
        # https://en.wikipedia.org/wiki/Block_Elements
        bar_chunks, remainder = divmod(int(count * 8 / increment), 8)

        # First draw the full width chunks
        bar = "█" * bar_chunks

        # Then add the fractional part.  The Unicode code points for
        # block elements are (8/8), (7/8), (6/8), ... , so we need to
        # work backwards.
        if remainder > 0:
            bar += chr(ord("█") + (8 - remainder))

        # If the bar is empty, add a left one-eighth block
        bar = bar or "▏"

        if count == 0:
            color = "grey"
        else:
            try:
                color = colors[label]
            except KeyError:
                color = None

        print(
            termcolor.colored(
                f"{label.ljust(longest_label_length).lower()}  {count:#6d} {bar}", color
            )
        )


def to_s(last_event):
    if last_event.days > 0 and last_event.seconds > 3600:
        return "%dd %dh" % (last_event.days, last_event.seconds // 3600)
    elif last_event.days > 0:
        return "%dd %ds" % (last_event.days, last_event.seconds)
    else:
        return "%ds" % (last_event.seconds)


def get_failure_description(events):
    descriptions = [ev["description"] for ev in events]

    if len(descriptions) == 1:
        return descriptions[0]

    failures = [
        desc for desc in descriptions if "failed -" in desc or desc == "Register failed"
    ]

    if len(failures) == 1:
        return failures[0]

    return descriptions[-1]


if __name__ == "__main__":
    KNOWN_FAILURES = {line.strip() for line in open("known_failures.txt")}

    def all_ingests():
        for ingest in cached_scan_iterator("storage-ingests"):
            if ingest["id"] in KNOWN_FAILURES:
                continue
            yield ingest

    data = {"Accepted": 0, "Failed": 0, "Completed": 0, "Processing": 0}

    failed = {}
    processing = {}

    NOW = dt.datetime.now()

    for item in all_ingests():
        status = item["payload"]["status"]

        try:
            last_event = max(
                event["createdDate"] for event in item["payload"]["events"]
            )
        except (KeyError, TypeError):
            last_event = item["payload"]["createdDate"]

        last_event = NOW - dt.datetime.fromtimestamp(int(last_event) / 1000)

        ingest_id = item["id"]

        if status == "Failed":
            failed[ingest_id] = {
                "date": last_event,
                "description": get_failure_description(item["payload"]["events"]),
            }
        if status == "Processing":
            processing[ingest_id] = last_event

        data[status] += 1

    if failed:
        for ingest_id, ingest_data in sorted(failed.items()):
            if ingest_data["description"].startswith(
                "Unpacking failed - There is no archive at"
            ):
                ingest_data[
                    "description"
                ] = "Unpacking failed - There is no archive at <src>"

        failed_by_reason = collections.defaultdict(list)

        for (ingest_id, ingest_data) in sorted(
            failed.items(), key=lambda t: t[1]["date"]
        ):
            failed_by_reason[ingest_data["description"]].insert(0, ingest_id)

        print("== failed ==")

        lines = []
        for reason, ingest_ids in sorted(
            failed_by_reason.items(), key=lambda t: -len(t[1])
        ):
            lines.append("%s:" % reason)
            for i_id in ingest_ids:
                lines.append("  %s" % i_id)
            lines.append("")

        print(termcolor.colored("\n".join(lines), "red"))
        print("== failed ==\n")

    if processing:
        print("== processing ==")

        lines = [
            "%s ~> %s" % (ingest_id, to_s(date))
            for (ingest_id, date) in sorted(processing.items(), key=lambda t: t[1])
        ]

        print(termcolor.colored("\n".join(lines[-5:]), "blue"))
        print("== processing ==\n")

    if all(v == 0 for v in data.values()):
        print("No ingests!")
    else:
        draw_chart(
            [
                (label, data[label])
                for label in ["Accepted", "Processing", "Completed", "Failed"]
            ],
            colors={
                "Accepted": "yellow",
                "Failed": "red",
                "Completed": "green",
                "Processing": "blue",
            },
        )
