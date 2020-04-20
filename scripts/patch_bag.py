#!/usr/bin/env python
"""
This script allows you to "patch" a bag, e.g. if you need to add an ALTO file
or swap out a corrupted image.

Use this responsibly!  If metadata inside the bag refers to the file you're
replacing (e.g. a checksum in the METS file), it will *not* be updated.

"""

import glob
import hashlib
import os
import subprocess
import tempfile
import uuid
import webbrowser

import bagit
import click
import termcolor

from common import get_aws_resource, get_read_only_aws_resource, get_storage_client


def print_info(message):
    print(termcolor.colored(message, "green"))


def create_empty_bag(bag):
    working_dir = tempfile.mkdtemp()
    bag_dir = os.path.join(working_dir, bag["info"]["externalIdentifier"])

    location = bag["location"]
    bucket = location["bucket"]
    path_prefix = location["path"]

    # Create a payload directory for new files to be stored in
    os.makedirs(os.path.join(bag_dir, "data"))
    os.makedirs(os.path.join(bag_dir, "data", "objects"))

    # Synthesize a new fetch file based on the files in the previous version
    # of the bag
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

        s3.Bucket(bucket).download_file(
            Key=f"{path_prefix}/{path}", Filename=os.path.join(bag_dir, name)
        )

    return bag_dir


def prepare_bag(bag_dir):
    payload_paths = set()

    for dirpath, _, filenames in os.walk(os.path.join(bag_dir, "data")):
        for f in filenames:
            if f == ".DS_Store":
                os.unlink(os.path.join(dirpath, f))
                continue

            payload_paths.add(os.path.relpath(os.path.join(dirpath, f), bag_dir))

    payload_oxum = {"count": 0, "size": 0}

    # Update entries for the new payload files
    for manifest_path in glob.glob(f"{bag_dir}/manifest-*.txt"):
        existing_lines = list(open(manifest_path))

        hasher = {
            "manifest-sha256.txt": hashlib.sha256(),
            "manifest-sha512.txt": hashlib.sha512(),
        }[os.path.basename(manifest_path)]

        with open(manifest_path, "w") as out_file:
            for line in existing_lines:
                checksum, path = line.strip().split("  ", 1)
                if path in payload_paths:
                    payload_oxum["count"] += 1
                    payload_oxum["size"] += os.stat(os.path.join(bag_dir, path)).st_size

                    hasher.update(open(os.path.join(bag_dir, path), "rb").read())

                    out_file.write(
                        # TODO: This will explode with out-of-memory on big files
                        hasher.hexdigest()
                        + "  "
                        + path
                        + "\n"
                    )
                else:
                    out_file.write(line)

    payload_oxum["count"] /= len(glob.glob(f"{bag_dir}/manifest-*.txt"))
    payload_oxum["size"] /= len(glob.glob(f"{bag_dir}/manifest-*.txt"))

    changed_files = payload_oxum["count"]

    # Remove the paths from the fetch.txt
    fetch_path = f"{bag_dir}/fetch.txt"
    existing_fetches = list(open(fetch_path))

    with open(f"{bag_dir}/fetch.txt", "w") as fetch_file:
        for line in existing_fetches:
            _, size, path = line.strip().split("\t", 2)
            if path not in payload_paths:
                payload_oxum["count"] += 1
                payload_oxum["size"] += int(size)

                fetch_file.write(line)

    # Construct the new bag
    bag = bagit.Bag(bag_dir)

    bag.info["Payload-Oxum"] = "%s.%s" % (payload_oxum["size"], payload_oxum["count"])
    bagit._make_tag_file("bag-info.txt", bag.info)

    for dirpath, _, filenames in os.walk(bag_dir):
        for f in filenames:
            if f == ".DS_Store":
                os.unlink(dirpath, f)

    files_for_tag_manifest = [
        f
        for f in os.listdir(bag_dir)
        if os.path.isfile(os.path.join(bag_dir, f)) and not f.startswith("tagmanifest-")
    ]

    for tagmanifest_path in glob.glob(f"{bag_dir}/tagmanifest-*.txt"):
        with open(tagmanifest_path, "w") as tag_file:
            for f in files_for_tag_manifest:
                hasher = {
                    "tagmanifest-sha256.txt": hashlib.sha256(),
                    "tagmanifest-sha512.txt": hashlib.sha512(),
                }[os.path.basename(tagmanifest_path)]

                path = os.path.join(bag_dir, f)

                hasher.update(open(path, "rb").read())
                checksum = hasher.hexdigest()
                tag_file.write(f"{checksum}  {f}\n")

    return changed_files


@click.command()
@click.option(
    "--space",
    required=True,
    type=click.Choice(
        ["digitised", "born-digital", "born-digital-accessions"], case_sensitive=False
    ),
    prompt="What is the external identifier of the bag you want to patch?",
)
@click.option(
    "--external-identifier",
    required=True,
    prompt="What is the external identifier of the bag you want to patch?",
)
def main(space, external_identifier):
    print_info(f"Patching bag {external_identifier} in the {space} space")

    bag = get_bag(space=space, external_identifier=external_identifier)
    print_info(f"Retrieved bag from storage service (version {bag['version']})")

    bag_dir = create_empty_bag(bag)
    subprocess.check_call(["open", bag_dir])
    print_info(f"Created a skeleton bag in {bag_dir}")
    print_info(f"Please add any payload files you want to replace")

    input("Press enter when you've added all the new files: ")

    changed_files = prepare_bag(bag_dir)

    click.confirm(
        "Ready to upload a new bag with %d replaced file%s?"
        % (changed_files, "s" if changed_files != 1 else ""),
        abort=True,
    )

    tar_path = os.path.join(os.path.dirname(bag_dir), f"{external_identifier}.tar.gz")

    subprocess.check_call(
        ["python3", "-m", "tarfile", "-c", tar_path, os.path.basename(bag_dir)],
        cwd=os.path.dirname(bag_dir),
    )

    bucket = "wellcomecollection-storage-prod-unpacked-bags"
    key = f"uploads/{external_identifier}.{uuid.uuid4()}.tar.gz"

    s3 = get_aws_resource(
        "s3", role_arn="arn:aws:iam::975596993436:role/storage-developer"
    )

    s3.Bucket(bucket).upload_file(Filename=tar_path, Key=key)

    client = get_storage_client(api_url="https://api.wellcomecollection.org/storage/v1")
    ingest_id = client.create_s3_ingest(
        space_id="digitised",
        s3_bucket=bucket,
        s3_key=key,
        external_identifier=external_identifier,
        ingest_type="update",
    ).split("/")[-1]

    inspector_url = f"https://wellcome-ingest-inspector.glitch.me/ingests/{ingest_id}"
    print_info(f"Ingest started: {inspector_url}")
    webbrowser.open(inspector_url)


def get_bag(space, external_identifier):
    client = get_storage_client(api_url="https://api.wellcomecollection.org/storage/v1")

    return client.get_bag(space_id=space, source_id=external_identifier)


if __name__ == "__main__":
    main()
