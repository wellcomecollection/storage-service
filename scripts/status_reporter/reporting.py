# -*- encoding: utf-8

import collections

import termcolor

import dynamo_status_manager


def has_succeeded_previously(row, name):
    return row.get(name, {}).get("success")


def get_named_status(row, name):
    stored_result = row.get(name, {}).get("success")

    if stored_result is None:
        return "not checked"
    elif stored_result:
        return "success"
    else:
        return "failure"


def _draw_ascii_bar_chart(data, colors=None):
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

        print(
            termcolor.colored(
                f"{label.rjust(longest_label_length)} ▏ {count:#6d} {bar}",
                colors.get(label),
            )
        )


def pprint_report(report):
    TOTAL_BNUMBERS = 268_605

    unknown = TOTAL_BNUMBERS - sum(report.values())

    data = [
        ("success", report["success"]),
        ("failure", report["failure"]),
        ("not checked", report["not checked"]),
        ("unknown", unknown),
    ]

    colors = {
        "success": "green",
        "failure": "red",
        "not checked": "blue",
        "unknown": "yellow",
    }

    for (label, count) in data:
        if count == 0:
            colors[label] = "grey"

    _draw_ascii_bar_chart(data, colors)


def build_report(name):
    reader = dynamo_status_manager.DynamoStatusReader()

    report = collections.Counter()

    for row in reader.all():
        status = get_named_status(row, name=name)
        report[status] += 1

    pprint_report(report)
