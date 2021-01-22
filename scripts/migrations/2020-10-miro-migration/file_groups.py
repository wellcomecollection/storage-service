#!/usr/bin/env python3
"""
This file contains functions related to assigning a file
a group name based on its path in S3
"""


def choose_group_name(prefix, s3_key):
    assert s3_key.startswith(prefix), f"{s3_key} does not start with {prefix}"

    s3_key_without_prefix = s3_key[len(prefix) :]

    split_key = list(filter(None, s3_key_without_prefix.split("/")))

    squishable_folders = {
        "Movies",
        "New_Scans_Edited_Images",
        "FP Images",
        "A Images",
        "AS Images",
        "C Scanned",
        "W Images",
    }

    corporate_photography_key = "Corporate_Photography"

    if prefix.startswith("miro/jpg_derivatives"):
        return "jpg" + "/" + split_key[0]

    if split_key[0] == corporate_photography_key:
        if len(split_key) > 2:
            return split_key[0] + "/" + split_key[1] + "/" + split_key[2]
        else:
            return split_key[0] + "/" + split_key[1]

    if split_key[0] in squishable_folders:
        return split_key[0]

    if len(split_key) == 2:
        return split_key[0]

    if len(split_key) > 2:
        return split_key[0] + "/" + split_key[1]

    return "no_group"


# if __name__ == "__main__":
#     from s3 import list_s3_objects_from
#
#     prefix = "miro/Wellcome_Images_Archive"
#
#     for s3_obj in list_s3_objects_from(
#         bucket="wellcomecollection-assets-workingstorage", prefix=prefix
#     ):
#         s3_key = s3_obj["Key"]
#
#         group_name = choose_group_name(prefix, s3_key)
#         print(group_name)
