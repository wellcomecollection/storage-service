class UnknownMiroID(Exception):
    pass


def parse_miro_id(s3_key):
    """
    Returns the Miro ID and shard from an S3 key.
    """
