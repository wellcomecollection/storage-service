import os
import datetime as dt
from collections import Counter


def _parse_date(date_string):
    try:
        return dt.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%S.%fZ")
    except ValueError:
        return dt.datetime.strptime(date_string, "%Y-%m-%dT%H:%M:%SZ")


def tally_event_descriptions(events, environment: str):
    """
    Iterates over a list of events, as received from the /ingests API.

    Adds three fields to each event:

    -   _count shows how many times an event with the same description has
        already occurred
    -   _is_repeated is True if an event with the same description has
        already occurred
    -   _is_unmatched_start is True if an event is an "X started" without a
        corresponding "X succeeded" or "X failed" event
    """

    running_counter = Counter()
    all_descriptions = Counter(ev["description"] for ev in events)

    for i, ev in enumerate(events):
        running_counter[ev["description"]] += 1
        ev["_count"] = running_counter[ev["description"]]

        ev["_repeated"] = all_descriptions[ev["description"]] > 1

        future_events = events[i+1:]
        assert ev not in future_events

        if ev["description"].endswith(" started"):
            expected_descriptions = (
                ev["description"].replace(" started", " succeeded"),
                ev["description"].replace(" started", " failed"),
            )

            ev["_is_unmatched_start"] = not any(
                f_ev["description"].startswith(expected_descriptions)
                for f_ev in future_events
            )
        else:
            ev["_is_unmatched_start"] = False
        
        ev["kibanaUrl"] = get_kibana_url(ev, environment)

    return events


def deduce_service_name_from_event_description(description):
    for prefix, service_name in [
        ("Aggregating replicas", "replica_aggregator"),
        ("Replicating to Amazon Glacier", "bag-replicator_glacier"),
        ("Replicating to Azure", "bag-replicator_azure"),
        ("Replicating to primary location", "bag-replicator_primary"),
        ("Register", "bag_register"),
        ("Verification (Azure)", "bag-verifier_azure"),
        ("Verification (Amazon Glacier)", "bag-verifier_glacier"),
        ("Verification (primary location)", "bag-verifier_primary"),
        ("Verification (pre-replicating to archive storage)", "bag-verifier-pre-replication"),
        ("Assigning bag version", "bag-versioner"),
        ("Unpacking", "bag-unpacker"),
    ]:
        if description.startswith(prefix):
            return service_name

    raise ValueError("Unable to deduce service name from description: {description!r}")


def get_kibana_url(event, api):
    namespace = {
        "production": "prod",
        "staging": "staging",
    }[api]

    try:
        ecs_service_name = deduce_service_name_from_event_description(event["description"])
    except ValueError:
        # Otherwise, we don't know what logs to redirect to.
        return ""

    service_name = f"storage-{namespace}-{ecs_service_name}"

    event_time = _parse_date(event["createdDate"])

    # Slop to account for timezone weirdness.  Although Kibana stores timestamps
    # in UTC, searches happen in your local timezone.  For Wellcome devs this
    # will always be BST, so this is "good enough" for now.
    #
    # Actually localising this properly is a faff.
    search_start = (event_time - dt.timedelta(minutes=85)).strftime("%Y-%m-%dT%H:%M")
    search_end = (event_time + dt.timedelta(minutes=65)).strftime("%Y-%m-%dT%H:%M")

    logs_index_pattern = "cb5ba262-ec15-46e3-a4c5-5668d65fe21f"

    return (
        "https://logging.wellcomecollection.org/app/kibana#/discover?_g="
        f"(refreshInterval:(pause:!t,value:0),time:(from:'{search_start}',to:'{search_end}'))&"
        f"_a=(columns:!(log),"
        # These are very chatty apps and probably not what we wanted -- errors in
        # these apps don't get surfaced in the ingest inspector.
        f"filters:!(('$state':(store:appState),meta:(alias:!n,disabled:!f,index:'{logs_index_pattern}',key:service_name,params:(query:{service_name}),type:phrase),query:(match_phrase:(service_name:{service_name})))),"
        f"index:'{logs_index_pattern}',interval:auto,sort:!(!('@timestamp',desc)))"
    )


def get_s3_url(source_location):
    bucket = source_location["bucket"]
    key = source_location["path"]

    return (
        f"https://s3.console.aws.amazon.com/s3/buckets/{bucket}"
        f"/{os.path.dirname(key)}/?tab=overview&prefixSearch={os.path.basename(key)}"
        f"&region=eu-west-1"
    )


def get_display_s3_url(source_location):
    bucket = source_location["bucket"]
    key = source_location["path"]

    # The key to an Archivematica source is usually some hideous long path
    # of the form:
    #
    #     born-digital/2199/f389/66f5/4077/bcf7/c14b/0fdc/66cd/SAWTC={uuid}.tar.gz
    #
    # Displaying the whole thing is completely unhelpful, so truncate it
    # to a shorter URL for display purposes.
    if "archivematica-ingests" in bucket:
        top_level, *_, filename = key.split("/")
        key = f"{top_level}/.../{filename}"

    return f"s3://{bucket}/{key}"


def get_last_updated_date(ingest):
    try:
        last_event = max(ingest["events"], key=lambda ev: ev["createdDate"])
    except ValueError:
        return ""

    return last_event["createdDate"]
