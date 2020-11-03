import re


class CannotGetMiroShard(Exception):
    pass


def choose_miro_shard(miro_id):
    """
    Decides the shards that a Miro ID belongs to.

    We group the Miro IDs into per-thousand "shards".  This is an organisational
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
    import os

    from miro_ids import parse_miro_id, IgnoreMiroIDError
    from s3 import list_s3_objects_from

    for s3_obj in list_s3_objects_from(
        bucket="wellcomecollection-assets-workingstorage",
        prefix="miro/Wellcome_Images_Archive",
    ):
        s3_key = s3_obj["Key"]

        try:
            miro_id = parse_miro_id(s3_key=s3_key)
        except IgnoreMiroIDError:
            continue

        # The S3 key encodes the shard used in the old Miro file share, for example
        #
        #   miro/Wellcome_Images_Archive/A Images/A0000000/A0000001-CS-LS.jp2
        #
        # Here the Miro ID is A0000001, and the shard is A0000000.  The shard in
        # S3 may be different from the one we choose.  That's not necessarily
        # wrong, but it's something we should highlight for manual inspection.
        s3_shard = os.path.basename(os.path.dirname(s3_key))
        chosen_shard = choose_miro_shard(miro_id)

        if s3_shard != chosen_shard:
            print(f"key          = {s3_key}")
            print(f"Miro ID      = {miro_id}")
            print(f"shard in S3  = {s3_shard}")
            print(f"shard chosen = {chosen_shard}")
            print("---")

        print(miro_id)
        print(s3_obj)
        miro_shard = choose_miro_shard(miro_id)
        print(miro_shard)
        break
