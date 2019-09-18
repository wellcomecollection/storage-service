#!/usr/bin/env python
# -*- encoding: utf-8

import datetime as dt
import json
import sys

import termcolor


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
    if last_event.days > 0:
        return "%dd %ds" % (last_event.days, last_event.seconds)
    else:
        return "%ds" % (last_event.seconds)


if __name__ == "__main__":
    try:
        ingests_dump = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <INGESTS_JSON_DUMP>")

    def all_ingests():
        with open(ingests_dump) as f:
            for line in f:
                yield json.loads(line)

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
            failed[ingest_id] = last_event
        if status == "Processing":
            processing[ingest_id] = last_event

        data[status] += 1

    if failed:
        print("== failed ==")

        lines = [
            "%s ~> %s" % (ingest_id, to_s(date))
            for (ingest_id, date) in sorted(failed.items(), key=lambda t: t[1])
        ]

        print(termcolor.colored("\n".join(lines[-5:]), "red"))
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
