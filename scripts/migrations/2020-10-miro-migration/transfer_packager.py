
import concurrent
import itertools
import os
import shutil

S3_DOWNLOAD_CONCURRENCY = 3


def _download_objects_from_s3(s3_client, target_folder, s3_bucket, s3_key_list):
    def _download_s3_file(key):
        file_name = key.split("/")[-1]
        file_location = os.path.join(target_folder, file_name)
        s3_client.download_file(s3_bucket, key, file_location)

    perform = _download_s3_file
    tasks_to_do = iter(s3_key_list)

    # From https://alexwlchan.net/2019/10/adventures-with-concurrent-futures/
    with concurrent.futures.ThreadPoolExecutor() as executor:
        # Schedule the first N futures.  We don't want to schedule them all
        # at once, to avoid consuming excessive amounts of memory.
        futures = {
            executor.submit(perform, task): task
            for task in itertools.islice(tasks_to_do, S3_DOWNLOAD_CONCURRENCY)
        }

        while futures:
            # Wait for the next future to complete.
            done, _ = concurrent.futures.wait(
                futures, return_when=concurrent.futures.FIRST_COMPLETED
            )

            for fut in done:
                futures.pop(fut)
                #print(f"The outcome of {original_task} is {fut.result()}")

            # Schedule the next set of futures.  We don't want more than N futures
            # in the pool at a time, to keep memory consumption down.
            for task in itertools.islice(tasks_to_do, len(done)):
                fut = executor.submit(perform, task)
                futures[fut] = task


def _compress_folder(target_folder, remove_folder=True):
    archive_name = shutil.make_archive(target_folder, "zip", target_folder)
    if remove_folder:
        shutil.rmtree(target_folder, ignore_errors=True)

    return archive_name


def _create_metadata(target_folder, group_name):
    metadata_folder = os.path.join(target_folder, 'metadata')
    metadata_file_location = os.path.join(metadata_folder, 'metadata.csv')

    os.makedirs(metadata_folder, exist_ok=True)

    metadata_lines = [
        "filename, group_name",
        f"objects/, {group_name}"
    ]

    metadata_contents = "\n".join(metadata_lines)

    with open(metadata_file_location, "w") as fp:
        fp.write(metadata_contents)


def create_transfer_package(s3_client, group_name, s3_bucket, s3_key_list):
    target_base_folder = "target"
    target_folder = os.path.join(target_base_folder, group_name)

    os.makedirs(target_folder, exist_ok=True)

    _download_objects_from_s3(
        s3_client=s3_client,
        target_folder=target_folder,
        s3_bucket=s3_bucket,
        s3_key_list=s3_key_list
    )

    _create_metadata(
        target_folder=target_folder,
        group_name=group_name
    )

    _compress_folder(
        target_folder=target_folder
    )


