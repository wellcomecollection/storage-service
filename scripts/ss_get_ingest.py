#!/usr/bin/env python3
# -*- encoding: utf-8
"""
Look up an ingest.  Usage:

    python ss_get_ingest.py <INGEST_ID> [--debug]

The script will attempt to find the ingest ID in both
the prod and staging APIs.

"""

import datetime as dt
import sys

import termcolor
from wellcome_storage_service import IngestNotFound

from common import get_logger, get_storage_client


logger = get_logger(__name__)


def lookup_ingest(ingest_id):
    logger.debug("Looking up ingest ID %s", ingest_id)

    api_variants = {"stage": "api-stage", "prod": "api"}

    for name, host in api_variants.items():
        logger.debug("Checking %s API", name)

        api_url = f"https://{host}.wellcomecollection.org/storage/v1"
        client = get_storage_client(api_url)

        try:
            ingest = client.get_ingest(ingest_id)
        except IngestNotFound:
            logger.debug("Not found in %s API", name)
        else:
            logger.debug("Found ingest in %s API:", name)
            return ingest

    logger.error("Could not find %s in either API!", ingest_id)
    sys.exit(1)


def pprint_date(ds):
    # Get the UTC offset for the current system
    UTC_OFFSET_TIMEDELTA = dt.datetime.now() - dt.datetime.utcnow()

    utc_dt_obj = dt.datetime.strptime(ds, "%Y-%m-%dT%H:%M:%S.%fZ")
    dt_obj = utc_dt_obj + UTC_OFFSET_TIMEDELTA

    if dt_obj.date() == dt.datetime.now().date():
        return dt_obj.strftime("today @ %H:%M:%S")
    elif (dt_obj - dt.timedelta(days=1)).date() == dt.datetime.now().date():
        return dt_obj.strftime("yesterday @ %H:%M:%S")
    else:
        return dt_obj.strftime("%Y-%m-%d @ %H:%M:%S")


if __name__ == "__main__":
    try:
        ingest_id = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <INGEST_ID>")

    ingest = lookup_ingest(ingest_id)

    fieldname_width = 12

    api_variant = "prod" if "api." in ingest["@context"] else "staging"
    print("api:\t\t%s" % api_variant)

    print(
        "source:\t\ts3://%s/%s"
        % (ingest["sourceLocation"]["bucket"], ingest["sourceLocation"]["path"])
    )
    print("space:\t\t%s" % ingest["space"]["id"])
    print("external ID:\t%s" % ingest["bag"]["info"]["externalIdentifier"])

    try:
        print("version:\t%s" % ingest["bag"]["info"]["version"])
    except KeyError:
        pass

    print("")

    print("events:", end="")

    for event in ingest["events"]:
        if "--timestamps" in sys.argv:
            print(
                "\t\t%s (%s)"
                % (event["description"], pprint_date(event["createdDate"]))
            )
        else:
            print("\t\t%s" % event["description"])

    print("")
    try:
        last_event_date = ingest["events"][-1]["createdDate"]
    except IndexError:
        last_event_date = ingest["createdDate"]

    delta = dt.datetime.utcnow() - dt.datetime.strptime(
        last_event_date, "%Y-%m-%dT%H:%M:%S.%fZ"
    )

    print("started at:\t%s" % pprint_date(ingest["createdDate"]))

    if ingest["events"]:
        last_event_date = pprint_date(last_event_date)
        if delta.seconds < 5:
            print("last event:\t%s (just now)" % last_event_date)
        elif delta.seconds < 60:
            print("last event:\t%s (%d seconds ago)" % (last_event_date, delta.seconds))
        elif 60 <= delta.seconds < 120:
            print("last event:\t%s (1 minute ago)" % last_event_date)
        elif delta.seconds < 60 * 60:
            print(
                "last event:\t%s (%d minutes ago)"
                % (last_event_date, int(delta.seconds / 60))
            )
        else:
            print("last event:\t%s" % last_event_date)

    print("")

    status = ingest["status"]["id"]
    colour = {
        "accepted": "yellow",
        "processing": "blue",
        "succeeded": "green",
        "failed": "red",
    }[status]

    print("status:\t\t%s" % termcolor.colored(status.upper(), colour))

    if status == "succeeded":
        print("")
        print("To look up the bag:")
        print("")

        try:
            print(
                "    python3 ss_get_bag.py %s %s %s"
                % (
                    ingest["space"]["id"],
                    ingest["bag"]["info"]["externalIdentifier"],
                    ingest["bag"]["info"]["version"],
                )
            )
        except KeyError:
            print(
                "    python3 ss_get_bag.py %s %s"
                % (ingest["space"]["id"], ingest["bag"]["info"]["externalIdentifier"])
            )

    sys.exit(0)
