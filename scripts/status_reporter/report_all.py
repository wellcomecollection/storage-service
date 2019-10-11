# -*- encoding: utf-8

import datetime as dt
import json

import check_names
from dynamo_status_manager import DynamoStatusReader
import reporting


def _is_manually_skipped(record):
    return record.get(check_names.MANUAL_SKIP, {}).get("success")


def _is_success(record):
    expected_checks = [
        name for name in check_names.ALL_CHECK_NAMES if name != check_names.MANUAL_SKIP
    ]

    return all(record.get(name, {}).get("success") for name in expected_checks)


def _is_processing(record):
    expected_checks = [
        name
        for name in check_names.ALL_CHECK_NAMES
        if name != check_names.MANUAL_SKIP and name != check_names.METS_EXISTS
    ]

    return any(record.get(name, {}).get("has_run") for name in expected_checks)


def _is_failed(record):
    return any(
        (
            record.get(name, {}).get("has_run")
            and record.get(name, {}).get("success") is False
        )
        for name in check_names.ALL_CHECK_NAMES
    )


class Statuses:
    inconsistent = "? Inconsistent"
    skipped = " ⃠ Skipped"
    failed = "✗ Failed"
    succeeded = "✓ Succeeded"
    processing = "▶ Processing"
    waiting = "▷ Waiting"


STATUSES = Statuses()


def get_overall_status(status_record):
    expected_checks = [
        name for name in check_names.ALL_CHECK_NAMES if name != check_names.MANUAL_SKIP
    ]

    if _is_manually_skipped(status_record):
        if _is_processing(status_record):
            # A record is either running through the pipeline or manually skipped,
            # but it should never be both.
            return STATUSES.inconsistent
        else:
            return STATUSES.skipped

    else:
        if _is_failed(status_record):
            return STATUSES.failed
        elif _is_success(status_record):
            return STATUSES.succeeded
        elif _is_processing(status_record):
            return STATUSES.processing
        else:
            return STATUSES.waiting


def run():
    reader = DynamoStatusReader()

    all_statuses = {
        STATUSES.inconsistent: [],
        STATUSES.skipped: [],
        STATUSES.failed: [],
        STATUSES.succeeded: [],
        STATUSES.processing: [],
        STATUSES.waiting: [],
    }

    for status_record in reader.all():
        overall_status = get_overall_status(status_record)
        all_statuses[overall_status].append(status_record["bnumber"])

    colors = {
        STATUSES.inconsistent: "red",
        STATUSES.skipped: "green",
        STATUSES.failed: "red",
        STATUSES.succeeded: "green",
        STATUSES.processing: "blue",
        STATUSES.waiting: "yellow",
    }

    for label in colors:
        if not all_statuses[label]:
            colors[label] = "grey"

    data = [
        (label, len(all_statuses[label]))
        for label in [
            STATUSES.waiting,
            STATUSES.processing,
            STATUSES.inconsistent,
            STATUSES.failed,
            STATUSES.skipped,
            STATUSES.succeeded,
        ]
    ]

    out_path = f"statuses-{dt.datetime.now().strftime('%Y-%m-%d_%H-%M-%S')}.json"

    with open(out_path, "w") as f:
        f.write(json.dumps(all_statuses, indent=2, sort_keys=True))

    reporting._draw_ascii_bar_chart(data, colors)
