import os


def choose_tags(bucket, key):
    """
    Given the bucket, key and size of an object, decide what tags (if any)
    should be applied to it.
    """
    storage_space, *_ = key.split("/")

    tags = []

    # Add a tag to MXF files in the "digitised" space.
    #
    # These are high-resolution video masters from A/V digitisation.  We keep
    # them around in case we need to re-encode the video in future, but we don't
    # access them day-to-day.
    #
    # Apply a tag so they can be lifecycled to a cold storage tier.
    _, file_extension = os.path.splitext(key)

    if storage_space == "digitised" and file_extension.lower() == ".mxf":
        tags.append(("Content-Type", "application/mxf"))

    return tags
