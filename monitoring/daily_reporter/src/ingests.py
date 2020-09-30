import datetime


def classify_ingest(ingest):
    """
    Get an ingest status that reflects whether we need to pay attention to it.
    For example:

    *   If the ingest has failed, was it a user error (failed verification) or
        a storage service error (failed replication)?
    *   If the ingest is processing or accepted, has it been updated recently,
        or is it stalled?

    """
    # Success never needs our attention.
    if ingest["status"] == "succeeded":
        return "succeeded"

    elif ingest["status"] == "failed":
        # We sort failures into two groups:
        #
        #   -   a user error is one that means there was something wrong with the
        #       bag, e.g. it couldn't be unpacked correctly, it failed verification
        #   -   an unknown error is one that we can't categorise, and might indicate
        #       a storage service error, e.g. a replication failure
        #
        failure_reasons = [
            ev["description"]
            for ev in ingest["events"]
            if "failed" in ev["description"]
        ]

        if failure_reasons and all(
            reason.startswith(
                (
                    "Verification (pre-replicating to archive storage) failed",
                    "Unpacking failed",
                    "Assigning bag version failed",
                )
            )
            for reason in failure_reasons
        ):
            return "failed (user error)"
        else:
            return "failed (unknown reason)"

    elif ingest["status"] == "accepted":
        # An ingest is in the 'accepted' state until it goes to the bag unpacker.
        # There may be a short delay while the bag unpacker starts up; a delay of
        # more than an hour suggests something is wrong.
        #
        # To allow for timezone slop, look for a delay of two hours.
        delay = datetime.datetime.now() - ingest["createdDate"]

        if abs(delay.total_seconds()) > 60 * 60 * 2:
            return "stalled"
        else:
            return "accepted"

    elif ingest["status"] == "processing":
        # Ingests should wait up to 5 hours before being retried due to SQS.
        # If an ingest hasn't been updated in more than 5 hours, something is
        # probably wrong.
        #
        # To allow for timezone slop, look for a delay of seven hours.  It will
        # be flagged the following day if it's still stalled.
        delay = datetime.datetime.now() - ingest["createdDate"]

        if abs(delay.total_seconds()) > 60 * 60 * 7:
            return "stalled"
        else:
            return "processing"
