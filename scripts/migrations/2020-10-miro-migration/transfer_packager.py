import concurrent
import itertools
import os

import attr
from boto3.s3.transfer import TransferConfig
from tqdm import tqdm

from common import (
    compress_folder,
    file_exists,
    slugify,
)
from s3 import (
    get_s3_object_size,
)

S3_DOWNLOAD_CONCURRENCY = 3


@attr.s
class TransferPackage:
    local_location = attr.ib()
    content_length = attr.ib()
    s3_location = attr.ib(default=None)


def _download_s3_object(s3_client, s3_bucket, s3_key, target_folder, prefix):
    assert s3_key.startswith(prefix), f"{s3_key} does not start with {prefix}"

    file_name = s3_key[len(prefix) :].strip("/")
    file_location = os.path.join(target_folder, file_name)

    s3_content_length = get_s3_object_size(
        s3_client=s3_client, s3_bucket=s3_bucket, s3_key=s3_key
    )

    def _download():
        required_dir = os.path.dirname(file_location)
        os.makedirs(required_dir, exist_ok=True)
        s3_client.download_file(Bucket=s3_bucket, Key=s3_key, Filename=file_location)

    # Check if we already have this file
    if os.path.isfile(file_location):
        # If the length doesn't match overwrite
        if os.path.getsize(file_location) != s3_content_length:
            _download()
    else:
        _download()

    file_exists(
        file_location=file_location, expected_content_length=s3_content_length
    )


def _download_objects_from_s3(s3_client, target_folder, s3_bucket, s3_key_list, prefix):
    def _get_s3_object(s3_key):
        _download_s3_object(
            s3_client=s3_client,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            target_folder=target_folder,
            prefix=prefix,
        )

    # itertools below expects an iterator
    s3_key_list_iter = iter(s3_key_list)

    with tqdm(total=len(s3_key_list)) as pbar:
        # From https://alexwlchan.net/2019/10/adventures-with-concurrent-futures/
        with concurrent.futures.ThreadPoolExecutor() as executor:
            # Schedule the first N futures.  We don't want to schedule them all
            # at once, to avoid consuming excessive amounts of memory.
            futures = {
                executor.submit(_get_s3_object, task): task
                for task in itertools.islice(s3_key_list_iter, S3_DOWNLOAD_CONCURRENCY)
            }

            while futures:
                # Wait for the next future to complete.
                done, _ = concurrent.futures.wait(
                    futures, return_when=concurrent.futures.FIRST_COMPLETED
                )

                for fut in done:
                    futures.pop(fut)

                pbar.update(len(done))

                # Schedule the next set of futures.  We don't want more than N futures
                # in the pool at a time, to keep memory consumption down.
                for task in itertools.islice(s3_key_list_iter, len(done)):
                    fut = executor.submit(_get_s3_object, task)
                    futures[fut] = task

        target_folder_count = sum(
            [len(files) for _, _, files in os.walk(target_folder)]
        )

        assert target_folder_count == len(s3_key_list), (
            f"Unexpected file count in {target_folder}: "
            f"{target_folder_count} != {len(s3_key_list)}"
        )


def _create_metadata(target_folder, group_name):
    metadata_folder = os.path.join(target_folder, "metadata")
    metadata_file_location = os.path.join(metadata_folder, "metadata.csv")

    os.makedirs(metadata_folder, exist_ok=True)

    metadata_lines = ["filename,dc.identifier,isMiro", f"objects/,{group_name},true"]

    metadata_contents = "\n".join(metadata_lines)

    with open(metadata_file_location, "w") as fp:
        fp.write(metadata_contents)


def upload_transfer_package(
    s3_client, s3_bucket, s3_path, transfer_package, cleanup=False
):
    file_location = transfer_package.local_location

    filename = os.path.basename(file_location)
    s3_key = f"{s3_path}/{filename}"

    bytes_to_upload = os.path.getsize(file_location)

    config = TransferConfig(
        multipart_threshold=1024 * 25,
        max_concurrency=10,
        multipart_chunksize=1024 * 25,
        use_threads=True
    )

    with tqdm(total=bytes_to_upload) as pbar:

        def _update_pbar(num_bytes):
            pbar.update(num_bytes)

        s3_client.upload_file(
            Filename=file_location,
            Bucket=s3_bucket,
            Key=s3_key,
            Callback=_update_pbar,
            Config=config
        )

        s3_content_length = get_s3_object_size(
            s3_client=s3_client, s3_bucket=s3_bucket, s3_key=s3_key
        )

        file_exists(
            file_location=file_location, expected_content_length=s3_content_length
        )

        if cleanup:
            os.remove(file_location)

    transfer_package.s3_location = {"s3_bucket": s3_bucket, "s3_key": s3_key}

    return transfer_package


def create_transfer_package(s3_client, group_name, s3_bucket, s3_key_list, prefix):
    target_base_folder = "target"
    folder_id = slugify(group_name)

    target_folder = os.path.join(target_base_folder, folder_id)

    os.makedirs(target_folder, exist_ok=True)

    _download_objects_from_s3(
        s3_client=s3_client,
        target_folder=target_folder,
        s3_bucket=s3_bucket,
        s3_key_list=s3_key_list,
        prefix=prefix,
    )

    _create_metadata(target_folder=target_folder, group_name=group_name)

    archive_location = compress_folder(target_folder=target_folder)
    archive_content_length = os.path.getsize(archive_location)

    return TransferPackage(
        local_location=archive_location, content_length=archive_content_length
    )
