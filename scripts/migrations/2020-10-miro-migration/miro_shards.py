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
    import sys

    from miro_ids import parse_miro_id, IgnoreMiroIDError
    from s3 import list_s3_objects_from

    mismatches = 0

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

        try:
            chosen_shard = choose_miro_shard(miro_id)
        except Exception:
            print(f"Cannot choose shard for {miro_id}", file=sys.stderr)
            raise

        # Some of the W images are in a directory called "Digital".  This isn't
        # a shard we'd want to preserve -- skip these.
        if miro_id.startswith("W") and s3_shard == "Digital":
            continue

        # The S images don't appear to be sharded at all.  For now, assume that's
        # fine and we're happy to use our own shards.
        if miro_id.startswith("S") and s3_shard == "S Images":
            continue

        # Ignore some L images which aren't sharded.  e.g.
        #
        #     .../New_Scans_Edited_Images/Orig_Jp2 already_/L0027175-LH-CS.jp2
        #
        if miro_id.startswith("L") and s3_shard in {
            "al_Tamsin_Graham_09-06-2015_11.45.00_Pre-Numbered",
            "Large Files",
            "Large files",
            "Orig_Jp2 already_",
            "Sort",
        }:
            continue

        if (
            miro_id.startswith("V")
            and s3_shard == "al_Londa_Schiebinger_09-06-2015_12.46.23_Pre-Numbered"
        ):
            continue

        if miro_id.startswith("B") and s3_shard == "Check":
            continue

        # Some of the images exist outside their other shards, e.g.
        #
        #     miro/Wellcome_Images_Archive/New_Scans_Edited_Images/V/V0044807.JP2
        #
        if os.path.dirname(s3_key).endswith(f"New_Scans_Edited_Images/{miro_id[0]}"):
            continue

        # The sharding used in the AS prefix uses inconsistent padding, e.g.
        #
        #     key          = .../AS Images/AS00023000/AS0023724-CS-LH.jp2
        #     Miro ID      = AS0023724
        #     shard in S3  = AS00023000
        #     shard chosen = AS0023000
        #
        # If they differ only by the zero padding, we can ignore them.
        if (
            miro_id.startswith("AS")
            and len(s3_shard) == len("AS00023000")
            and len(chosen_shard) == len("AS0023000")
            and s3_shard.replace("AS000", "AS00") == chosen_shard
        ):
            continue

        # Some of the N images need sorting, e.g.
        #
        #     miro/Wellcome_Images_Archive/N Images/N0029000/Need to sort/N0029174.jp2
        #
        # If the next shard up is correct, we can ignore the difference.
        if (
            miro_id.startswith("N")
            and s3_shard == "Need to sort"
            and os.path.dirname(s3_key).endswith(f"/{chosen_shard}/Need to sort")
        ):
            continue

        # Ditto M images, e.g.
        #
        #     miro/Wellcome_Images_Archive/M Images/M0014000/Sort/M0014431-LH-RA.jp2
        #
        # If the next shard up is correct, we can ignore the difference.
        if (
            miro_id.startswith("M")
            and s3_shard == "Sort"
            and os.path.dirname(s3_key).endswith(f"/{chosen_shard}/Sort")
        ):
            continue

        # The S3 and chosen shard don't match.  Flag it for attention.
        if s3_shard != chosen_shard:
            if mismatches:
                print("---")

            print(f"key          = {s3_key}")
            print(f"Miro ID      = {miro_id}")
            print(f"shard in S3  = {s3_shard}")
            print(f"shard chosen = {chosen_shard}")

            mismatches += 1

    print(f"\nThere were {mismatches} mismatched shards for inspection")
