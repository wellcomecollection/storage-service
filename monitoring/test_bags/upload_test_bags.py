#!/usr/bin/env python

import os

import bagit
import boto3
import tarfile


def find_bag_dirs(root):
    for dir_entry in os.listdir(root):
        if os.path.isdir(dir_entry):
            yield dir_entry


def create_tar_gz(bag_dir):
    archive_name = f"{bag_dir}.tar.gz"

    with tarfile.open(f"{bag_dir}.tar.gz", "w:gz") as tf:
        for dirpath, _, filenames in os.walk(bag_dir):
            for f in filenames:
                name = os.path.join(dirpath, f)
                arcname = os.path.relpath(name, start=bag_dir)
                tf.add(name=name, arcname=arcname)

    return archive_name


def upload_tar_gz(tar_gz_name):
    s3 = boto3.client("s3")

    s3.upload_file(
        Bucket="wellcomecollection-storage-infra",
        Filename=tar_gz_name,
        Key=f"test_bags/{tar_gz_name}",
    )


if __name__ == "__main__":
    s3 = boto3.client("s3")

    for bag_dir in find_bag_dirs("."):
        print(f"Working on {bag_dir}")
        tar_gz_name = create_tar_gz(bag_dir)
        upload_tar_gz(tar_gz_name)
