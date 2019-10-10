# -*- encoding: utf-8

import collections

import termcolor

from check_names import ALL_CHECK_NAMES
import dynamo_status_manager


def has_succeeded_previously(status_summary, name):
    return status_summary.get(name, {}).get("success")


def get_named_status(status_summary, name):
    if name not in ALL_CHECK_NAMES:
        raise Exception(f"{name} is invalid, must be one of {ALL_CHECK_NAMES}")

    stored_result = status_summary.get(name, None)

    if stored_result is None:
        return "not checked"
    elif not stored_result["has_run"]:
        return "not checked"
    elif stored_result["success"]:
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
                f"{label.ljust(longest_label_length)} ▏ {count:#6d} {bar}",
                colors.get(label),
            )
        )

def pprint_status_summary(status_summary):
    title = f"Reporting on: {status_summary['bnumber']}"
    print(termcolor.colored(title, 'white', attrs=['bold']))

    for name in ALL_CHECK_NAMES:
        success = status_summary[name].get('success', False)
        has_run = status_summary[name].get('has_run', False)

        if(success and has_run):
            status_check = termcolor.colored("✓ Succeeded", 'green')
        elif(not success and has_run):
            status_check = termcolor.colored("✗ Failed", 'red')
        elif(not success and not has_run):
            status_check = termcolor.colored("✌︎ Not checked", 'blue')
        else:
            status_check = termcolor.colored("?︎ Inconsistent", 'yellow')

        print(f"{name.ljust(40)}{status_check}")


def pprint_report(report, title):
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

    print('')
    print(termcolor.colored(title, 'white', attrs=['bold']))
    _draw_ascii_bar_chart(data, colors)

def _get_reader():
    reader = dynamo_status_manager.DynamoStatusReader()
    return reader.all()

def generate_full_report():
    return list(_get_reader())

def build_report(name, report=None):
    if report is None:
        report = _get_reader()

    report_count = collections.Counter()

    for status_summary in report:
        status = get_named_status(status_summary, name=name)
        report_count[status] += 1

    pprint_report(report_count, name)
