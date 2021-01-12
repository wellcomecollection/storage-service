#!/usr/bin/env python3
"""
Handle missing_images packaging
"""

import json

from common import get_aws_client
from iter_helpers import chunked_iterable
from s3 import list_s3_objects_from
from registrations import stored_files
from transfer_packager import create_transfer_package
from chunk_transfer import upload_chunk_package
from uploads import copy_transfer_package
from dlcs import register_image_batch

from decisions import get_decisions

PLATFORM_ROLE_ARN = "arn:aws:iam::760097843905:role/platform-read_only"
LORIS_IMAGES_BUCKET = "wellcomecollection-miro-images-public"
MISSING_IMAGE_LOCATION = "./registration_clearup/missing_miro_ids.json"
WORKING_STORAGE_BUCKET = "wellcomecollection-assets-workingstorage"
MIRO_METADATA_PREFIX = "miro/source_data"

def build_miro_metadata_transfer_package():
    s3_client = get_aws_client("s3", role_arn=PLATFORM_ROLE_ARN)

    miro_metadata_listing_keys = [result['Key'] for result in list_s3_objects_from(
        s3_client=s3_client,
        bucket=WORKING_STORAGE_BUCKET,
        prefix=MIRO_METADATA_PREFIX
    ) ]

    transfer_package = create_transfer_package(
        s3_client=s3_client,
        group_name="miro_metadata",
        s3_bucket=WORKING_STORAGE_BUCKET,
        s3_key_list=miro_metadata_listing_keys,
        prefix=MIRO_METADATA_PREFIX
    )

    # Created and sent on 17/12/20
    # transfer_package = upload_chunk_package(transfer_package)
    # transfer_package = copy_transfer_package(transfer_package)

    print(transfer_package)


def build_missing_images_transfer_package():
    with open(MISSING_IMAGE_LOCATION, 'r') as file:
        missing_images = json.loads(file.read())

    s3_client = get_aws_client("s3", role_arn=PLATFORM_ROLE_ARN)

    loris_images_listing = list_s3_objects_from(
        s3_client=s3_client,
        bucket=LORIS_IMAGES_BUCKET,
        prefix=""
    )

    found_images = {}

    for image in loris_images_listing:
        key = image['Key']
        miro_id = key.split(".")[-2]
        miro_id = miro_id.split("/")[-1]

        if miro_id in missing_images:
            found_images[miro_id] = key

    assert len(found_images) == len(missing_images), (
        f"Could not find all files in s3://{LORIS_IMAGES_BUCKET}"
    )

    transfer_package = create_transfer_package(
        s3_client=s3_client,
        group_name="miro_missing/loris",
        s3_bucket=LORIS_IMAGES_BUCKET,
        s3_key_list=found_images.values(),
        prefix=""
    )

    # Created and sent on 17/12/20
    # transfer_package = upload_chunk_package(transfer_package)
    # transfer_package = copy_transfer_package(transfer_package)

    print(transfer_package)

def register_missing_miro_ids():
    with open(MISSING_IMAGE_LOCATION, 'r') as file:
        missing_images = json.loads(file.read())

        registrations = []

        for file in stored_files():
            filename = file['_source']['name']
            miro_id = filename.split(".")[-2]
            miro_id = miro_id.split("/")[-1]

            if miro_id in missing_images:
                registrations.append({
                    'miro_id': miro_id,
                    'file_id': file['_id']
                })

    assert len(registrations) == len(missing_images), (
        f"Incorrect registration count, "
        f"expected {len(missing_images)}, "
        f"got {len(registrations)}"
    )

    register_batches = chunked_iterable(registrations, 25)
    
    for batch in register_batches:
       batch_result = register_image_batch(batch)
       print(batch_result)

if __name__ == "__main__":
    #build_missing_images_transfer_package()
    #build_miro_metadata_transfer_package()
    register_missing_miro_ids()


