#!/usr/bin/env python3
"""
This is a script to track the Azure replicator progress of Chemist & Druggist,
which has ~1M files in the bag.

Because blobs are written sequentially, it does a binary search to find
the last blob that was written, and report the replication progress

If the script fails, run `az login` to authenticate with Azure.

"""

import datetime
import json
import subprocess
import sys


def exists_in_azure(filename):
    cmd = [
        "az",
        "storage",
        "blob",
        "exists",
        "--account-name",
        "wecostorageprod",
        "--container-name",
        "wellcomecollection-storage-replica-netherlands",
        "--name",
        f"digitised/b19974760/v1/{filename}",
    ]
    resp = subprocess.check_output(cmd, stderr=subprocess.DEVNULL)
    return json.loads(resp)["exists"]


def show_azure_blob(filename):
    """
    Get the Azure info for a file in C&D.
    """
    cmd = [
        "az",
        "storage",
        "blob",
        "show",
        "--account-name",
        "wecostorageprod",
        "--container-name",
        "wellcomecollection-storage-replica-netherlands",
        "--name",
        f"digitised/b19974760/v1/{filename}",
    ]
    resp = subprocess.check_output(cmd, stderr=subprocess.DEVNULL)
    return json.loads(resp)


def get_chemist_and_druggist_files():
    files = []

    for line in open("chemist_druggist.txt"):  # tag manifest from S3
        checksum, filename = line.strip().split("  ")
        files.append(filename)

    return sorted(files)


def find_last_replicated_file(files):
    iterations = 0

    try:
        start = int(sys.argv[1]) - 1  # How far did it get last time?
    except IndexError:
        start = 0

    end = len(files)

    while start != end and start != end - 1:
        midpoint = int((start + end) / 2)
        filename = files[midpoint]
        does_it_exist_yet = exists_in_azure(filename)

        iterations += 1

        if does_it_exist_yet:
            print("✅", filename)
            start = midpoint
        else:
            print("❌", filename)
            end = midpoint

    if iterations > 1:
        print(f"Found in {iterations} steps")

    return start, files[start]


if __name__ == "__main__":
    files = get_chemist_and_druggist_files()

    position, last_replicated_file = find_last_replicated_file(files)

    print("")
    print(f"The latest file to be written to Azure is\n  {last_replicated_file}")

    print("")
    created_at = datetime.datetime.strptime(
        show_azure_blob(last_replicated_file)["properties"]["creationTime"],
        "%Y-%m-%dT%H:%M:%S+00:00",
    )

    # The point of this log is to highlight if the process has stalled and
    # blobs are no longer being written.
    if (datetime.datetime.utcnow() - created_at).seconds < 180:
        print("It was written just now")
    else:
        print(f"It was written at {created_at} UTC")

    print("")
    print(
        f"This means {round(position/len(files)*100, 1)}% "
        f"({position} / {len(files)}) of files have been replicated"
    )
