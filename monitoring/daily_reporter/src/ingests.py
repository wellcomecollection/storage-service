import datetime


def is_callback_404(description):
    # e.g. Callback failed for: abcd, got 404 Not Found!
    return description.startswith("Callback failed for:") and description.endswith(
        "got 404 Not Found!"
    )


def is_user_error(reason):
    if reason.startswith(
        (
            "Verification (pre-replicating to archive storage) failed",
            "Detecting bag root failed",
            # If we can't unpack a bag or assign a version for an
            # unknown reason, we should treat that as a storage service
            # error - hence requiring the hyphen which indicates a
            # longer explanation.
            "Unpacking failed -",
            "Assigning bag version failed -",
        )
    ):
        return True

    # This may mean that the user supplied us with a bad callback.
    #
    # More likely, this is a Goobi bag that got restarted.  Goobi callbacks
    # are only valid for ~72 hours, so if the bag gets stuck for some
    # reason you'll get this error.
    if reason.startswith("Callback failed for") and reason.endswith("got 401 Unauthorized!"):
        return True

    return False


def get_dev_status(ingest):
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
            if "failed" in ev["description"] and not is_callback_404(ev["description"])
        ]

        if failure_reasons and all(is_user_error(reason) for reason in failure_reasons):
            return "failed (user error)"
        else:
            return "failed (unknown reason)"

    elif ingest["status"] == "accepted":
        # An ingest is in the 'accepted' state until it goes to the bag unpacker.
        # There may be a short delay while the bag unpacker starts up.
        #
        # The visibility timeout on the queue is 5 hours -- we might see
        # no activity if there are two instances of the bag unpacker, and
        # one picks up the message just before stopping.
        #
        # To allow for timezone slop, look for a delay of 6 hours.
        delay = datetime.datetime.now() - ingest["createdDate"]

        if abs(delay.total_seconds()) > 60 * 60 * 6:
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
