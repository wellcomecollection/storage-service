import concurrent
import itertools
import os
import shutil

S3_DOWNLOAD_CONCURRENCY = 3


def _assert_local_content_length(file_location, expected_content_length):
    # Assert we have the correct number of bytes
    local_content_length = os.path.getsize(file_location)

    # Check that content length is non-zero
    assert local_content_length > 0

    assert (
        local_content_length == expected_content_length
    ), f"Content length mismatch: {file_location}!"


def _get_s3_content_length(s3_client, s3_bucket, s3_key):
    s3_head_object_response = s3_client.head_object(Bucket=s3_bucket, Key=s3_key)

    return s3_head_object_response["ContentLength"]


def _download_s3_object(s3_client, s3_bucket, s3_key, target_folder):
    file_name = s3_key.split("/")[-1]
    file_location = os.path.join(target_folder, file_name)

    s3_content_length = _get_s3_content_length(
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

    _assert_local_content_length(
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

    s3_client.upload_file(Filename=file_location, Bucket=s3_bucket, Key=s3_key)

    s3_content_length = _get_s3_content_length(
        s3_client=s3_client, s3_bucket=s3_bucket, s3_key=s3_key
    )

    _assert_local_content_length(
        file_location=file_location, expected_content_length=s3_content_length
    )

    if cleanup:
        os.remove(file_location)


def create_transfer_package(s3_client, group_name, s3_bucket, s3_key_list):
    target_base_folder = "target"
    target_folder = os.path.join(target_base_folder, group_name)

    os.makedirs(target_folder, exist_ok=True)

    _download_objects_from_s3(
        s3_client=s3_client,
        target_folder=target_folder,
        s3_bucket=s3_bucket,
        s3_key_list=s3_key_list,
    )

    _create_metadata(target_folder=target_folder, group_name=group_name)

    return _compress_folder(target_folder=target_folder)