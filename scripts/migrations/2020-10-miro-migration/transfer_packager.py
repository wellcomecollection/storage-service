import concurrent
import itertools
import os
import shutil

from tqdm import tqdm

from s3 import get_s3_content_length

S3_DOWNLOAD_CONCURRENCY = 3


def _check_local_content_length(file_location, expected_content_length):
    """
    Check that a file has the expected content length in bytes
    """
    local_content_length = os.path.getsize(file_location)

    assert local_content_length > 0, "Content length is zero: " f"{file_location}"

    assert local_content_length == expected_content_length, (
        "Content length mismatch "
        f"({local_content_length} != {expected_content_length}): "
        f"{file_location}"
    )


def _download_s3_object(s3_client, s3_bucket, s3_key, target_folder):
    file_name = s3_key.split("/")[-1]
    file_location = os.path.join(target_folder, file_name)

    s3_content_length = get_s3_content_length(
        s3_client=s3_client, s3_bucket=s3_bucket, s3_key=s3_key
    )

    def _download():
        s3_client.download_file(Bucket=s3_bucket, Key=s3_key, Filename=file_location)

    # Check if we already have this file
    if os.path.isfile(file_location):
        # If the length doesn't match overwrite
        if not os.path.getsize(file_location) == s3_content_length:
            _download()
    else:
        _download()

    _check_local_content_length(
        file_location=file_location, expected_content_length=s3_content_length
    )


def _download_objects_from_s3(s3_client, target_folder, s3_bucket, s3_key_list):
    def _get_s3_object(s3_key):
        _download_s3_object(
            s3_client=s3_client,
            s3_bucket=s3_bucket,
            s3_key=s3_key,
            target_folder=target_folder,
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

        assert len(os.listdir(target_folder)) == len(
            s3_key_list
        ), f"Unexpected file count in {target_folder}!"


def _compress_folder(target_folder, remove_folder=True):
    archive_name = shutil.make_archive(target_folder, "zip", target_folder)
    if remove_folder:
        shutil.rmtree(target_folder, ignore_errors=True)

    return archive_name


def _create_metadata(target_folder, group_name):
    metadata_folder = os.path.join(target_folder, "metadata")
    metadata_file_location = os.path.join(metadata_folder, "metadata.csv")

    os.makedirs(metadata_folder, exist_ok=True)

    metadata_lines = ["filename,dc.identifier,isMiro", f"objects/,{group_name},true"]

    metadata_contents = "\n".join(metadata_lines)

    with open(metadata_file_location, "w") as fp:
        fp.write(metadata_contents)


def upload_transfer_package(
    s3_client, s3_bucket, s3_path, file_location, cleanup=False
):
    filename = os.path.basename(file_location)
    s3_key = f"{s3_path}/{filename}"

    bytes_to_upload = os.path.getsize(file_location)

    with tqdm(total=bytes_to_upload) as pbar:

        def _update_pbar(num_bytes):
            pbar.update(num_bytes)

        s3_client.upload_file(
            Filename=file_location, Bucket=s3_bucket, Key=s3_key, Callback=_update_pbar
        )

        s3_content_length = _get_s3_content_length(
            s3_client=s3_client, s3_bucket=s3_bucket, s3_key=s3_key
        )

        _check_local_content_length(
            file_location=file_location, expected_content_length=s3_content_length
        )

        if cleanup:
            os.remove(file_location)

    return {"s3_content_length": s3_content_length, "s3_key": s3_key}


def create_transfer_package(s3_client, group_name, s3_bucket, s3_key_list):
    target_base_folder = "target"
    target_folder = os.path.join(target_base_folder, group_name)

    os.makedirs(target_folder, exist_ok=True)

    # TODO: We must account for downloading multiple same name files and not overwrite
    _download_objects_from_s3(
        s3_client=s3_client,
        target_folder=target_folder,
        s3_bucket=s3_bucket,
        s3_key_list=s3_key_list,
    )

    _create_metadata(target_folder=target_folder, group_name=group_name)

    return _compress_folder(target_folder=target_folder)
