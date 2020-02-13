#!/usr/bin/env python
"""
Create a local "clone" of a bag which has a fetch.txt pointing back to
every file in the previous version of the bag.

You can then add new files to this bag or edit it as appropriate, allowing
you to do a shallow update of the bag.
"""

import os
import re
import sys

import bagit

from common import get_read_only_aws_resource, get_storage_client


def slugify(ustr):
    """Convert Unicode string into an ASCII slug.

    Based on http://www.leancrew.com/all-this/2014/10/asciifying/
    """
    ustr = re.sub("[–—/:;,.]", "-", ustr)  # replace separating punctuation
    astr = ustr.lower()  # lowercase
    astr = re.sub(r"[^a-z0-9 -]", "", astr)  # delete any other characters
    astr = astr.replace(" ", "-")  # spaces to hyphens
    astr = re.sub(r"-+", "-", astr)  # condense repeated hyphens
    return astr


def clone_bag(api_name, space, external_identifier):
    api_url = {
        "prod": "https://api.wellcomecollection.org/storage/v1",
        "staging": "https://api-stage.wellcomecollection.org/storage/v1",
    }[api_name]

    client = get_storage_client(api_url)

    bag = client.get_bag(space_id=space, source_id=external_identifier)

    os.makedirs("_bags", exist_ok=True)

    bag_slug = f"{space}_{slugify(external_identifier)}"

    if api_name == "staging":
        bag_slug = f"staging_{bag_slug}"

    bag_dir = os.path.join("_bags", bag_slug)

    try:
        os.makedirs(bag_dir)
    except FileExistsError:
        sys.exit("You already have a clone of this bag!")

    os.makedirs(os.path.join(bag_dir, "data"))

    location = bag["location"]
    bucket = location["bucket"]
    path_prefix = location["path"]

    with open(os.path.join(bag_dir, "fetch.txt"), "w") as fetch_file:
        for manifest_file in sorted(bag["manifest"]["files"], key=lambda f: f["name"]):
            path = manifest_file["path"]
            size = manifest_file["size"]
            name = manifest_file["name"]

            fetch_file.write(f"s3://{bucket}/{path_prefix}/{path}\t{size}\t{name}\n")

    # Download some of the manifest files from the original bag.
    s3 = get_read_only_aws_resource("s3")

    for tag_manifest_file in bag["tagManifest"]["files"]:
        path = tag_manifest_file["path"]
        name = tag_manifest_file["name"]

        if name.startswith("tagmanifest-"):
            continue

        s3.Bucket(bucket).download_file(
            Key=f"{path_prefix}/{path}", Filename=os.path.join(bag_dir, name)
        )

    bag = bagit.Bag(bag_dir)
    bag.save(manifests=True)

    print(f"✨ Created your new bag at {bag_dir} ✨")
    print("")
    print("You can use the bagit-python tool to validate your new bag:")
    print("")
    print(f"$ bagit.py --validate {bag_dir}")
    print("")


if __name__ == "__main__":


    clone_bag("prod", "digitised", sys.argv[1])
