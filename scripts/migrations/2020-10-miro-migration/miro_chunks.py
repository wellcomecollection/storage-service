import re


class CannotGetMiroChunk(Exception):
    pass


def choose_miro_chunk(miro_id):
    """
    Decides the chunk that a Miro ID belongs to.

    We group the Miro IDs into per-thousand "chunks".  This is an organisational
    scheme inherited from the way Miro assets were laid out from disk in the old
    Miro fileshare.

    We reconstruct them ourselves -- they weren't always consistent or correct
    in the old scheme, but we try to stay as close to the previous scheme as possible.
    """
    match = re.match(r"(?P<letter_prefix>[A-Z]{1,2})(?P<digits>\d{4,})", miro_id)

    letter_prefix = match.group("letter_prefix")
    numeric_chunk = match.group("digits")[:4] + "000"

    return letter_prefix + numeric_chunk


if __name__ == "__main__":
    from miro_ids import parse_miro_id, IgnoreMiroIDError
    from s3 import list_s3_objects_from

    for s3_obj in list_s3_objects_from(
        bucket="wellcomecollection-assets-workingstorage",
        prefix="miro/Wellcome_Images_Archive",
    ):
        try:
            miro_id = parse_miro_id(s3_key=s3_obj["Key"])
        except IgnoreMiroIDError:
            continue
        print(miro_id)
        print(s3_obj)
        miro_chunk = choose_miro_chunk(miro_id)
        print(miro_chunk)
        break
