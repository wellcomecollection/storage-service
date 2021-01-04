import datetime

import pytest

from ingests import get_dev_status


@pytest.mark.parametrize(
    "ingest, expected_dev_status",
    [
        ({"status": "succeeded"}, "succeeded"),
        (
            {
                "status": "accepted",
                "createdDate": datetime.datetime.now() - datetime.timedelta(seconds=5),
            },
            "accepted",
        ),
        (
            {
                "status": "accepted",
                "createdDate": datetime.datetime.now() - datetime.timedelta(days=5),
            },
            "stalled",
        ),
        (
            {
                "status": "processing",
                "createdDate": datetime.datetime.now() - datetime.timedelta(seconds=5),
            },
            "processing",
        ),
        (
            {
                "status": "processing",
                "createdDate": datetime.datetime.now() - datetime.timedelta(days=5),
            },
            "stalled",
        ),
        (
            {
                "status": "failed",
                "events": [{"description": "Assigning bag version failed"}],
            },
            "failed (unknown reason)",
        ),
        (
            {
                "status": "failed",
                "events": [
                    {
                        "description": "Assigning bag version failed - newer version of bag etc."
                    }
                ],
            },
            "failed (user error)",
        ),
        (
            {
                "status": "failed",
                "events": [{"description": "Unpacking failed - the bag doesn't exist"}],
            },
            "failed (user error)",
        ),
        (
            {"status": "failed", "events": [{"description": "Unpacking failed"}]},
            "failed (unknown reason)",
        ),
        (
            {"status": "failed", "events": []},
            "failed (unknown reason)",
        ),
        # e.g. https://wellcome-ingest-inspector.glitch.me/ingests/fd6c1d57-5f1a-4268-894a-67add059b6ed
        (
            {
                "status": "failed",
                "events": [
                    {"description": "Unpacking failed"},
                    {"description": "Unpacking failed"},
                    {"description": "Unpacking failed - the bag doesn't exist"}],
            },
            "failed (user error)",
        ),
    ],
)
def test_get_dev_status(ingest, expected_dev_status):
    assert get_dev_status(ingest) == expected_dev_status
