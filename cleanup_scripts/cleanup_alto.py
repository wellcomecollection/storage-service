#!/usr/bin/env python

import datetime
import functools
import hashlib
import json
import os
import re
import sys
import termcolor

import tqdm
from wellcome_storage_service import StorageServiceClient


NOW = datetime.datetime.now().isoformat().replace(":", "_")


def get_alto_paths(root):
    for dirpath, _, filenames in os.walk(root):
        if not dirpath.endswith("_alto"):
            continue

        for f in sorted(filenames):
            if not f.endswith(".xml"):
                continue

            yield (f, os.path.join(dirpath, f))


def get_storage_client(api_url):
    creds_path = os.path.join(
        os.environ["HOME"], ".wellcome-storage", "oauth-credentials.json"
    )
    oauth_creds = json.load(open(creds_path))

    return StorageServiceClient(
        api_url=api_url,
        client_id=oauth_creds["client_id"],
        client_secret=oauth_creds["client_secret"],
        token_url=oauth_creds["token_url"],
    )


@functools.lru_cache()
def get_bag(b_number):
    client = get_storage_client("https://api.wellcomecollection.org/storage/v1")

    return client.get_bag(space_id="digitised", source_id=b_number)


@functools.lru_cache()
def get_xml_files(b_number):
    bag = get_bag(b_number)

    return {
        os.path.basename(f["name"]): {"size": f["size"], "checksum": f["checksum"]}
        for f in bag["manifest"]["files"]
        if f["name"].endswith(".xml")
    }


@functools.lru_cache()
def checksum(path):
    h = hashlib.sha256()
    h.update(open(path, "rb").read())
    return h.hexdigest()


@functools.lru_cache()
def size(path):
    return os.stat(path).st_size


def warn(s):
    print(termcolor.colored(s, "yellow"), file=sys.stderr)


def log_event(s):  # pragma: no cover
    with open(f"alto_cleanup_{NOW}.log", "a") as out_file:
        out_file.write(s.rstrip() + "\n")


# Trying to look for the b number in an ALTO filename, e.g.
#
#     b21020000_0001.xml
#
B_NUMBER_RE = re.compile(r"^(?P<b_number>b[0-9]{7}[0-9x])")


def get_info_blobs(paths):
    for alto_name, alto_path in paths:
        try:
            b_number = B_NUMBER_RE.match(alto_name).group("b_number")
        except AttributeError:
            warn(f"Unable to identify b number in {alto_path!r}")
            continue

        files = get_xml_files(b_number)

        try:
            matching_file = files[alto_name]
        except KeyError:
            warn(f"Unable to find file in storage manifest for {alto_path!r}")
            continue

        yield {
            "name": alto_name,
            "path": alto_path,
            "b_number": b_number,
            "alto_size": size(alto_path),
            "stored_size": matching_file["size"],
            "alto_checksum": checksum(alto_path),
            "stored_checksum": matching_file["checksum"],
        }


def get_paths_for_deletion(info_blobs):
    for info in info_blobs:
        if info["alto_size"] != info["stored_size"]:
            warn(
                f"Sizes don't match:\nPath       = %s\nMETS share = %10d\nStorage    = %10d"
                % (info["path"], info["alto_size"], info["stored_size"])
            )
            continue

        if info["alto_checksum"] != info["stored_checksum"]:
            warn(
                f"Checksums don't match:\nPath       = %s\nMETS share = %s\nStorage    = %s\n"
                % (info["path"], info["alto_checksum"], info["stored_checksum"])
            )
            continue

        yield (info["path"], info["alto_size"])


def run_deleter(root):
    paths = get_alto_paths(root)
    info_blobs = get_info_blobs(paths)
    paths_for_deletion = get_paths_for_deletion(info_blobs)

    for path, size in tqdm.tqdm(paths_for_deletion):
        log_event(json.dumps({"event": "delete", "path": path, "size": size,}))
        os.unlink(path)


if __name__ == "__main__":  # pragma: no cover
    root = "/Volumes/Shares/LIB_WDL_DDS/LIB_WDL_DDS_METS/"

    run_deleter(root)
    #
    # paths = get_alto_paths(root)
    # info_blobs = get_info_blobs(paths)
    # paths_for_deletion = get_paths_for_deletion(info_blobs)
    #
    # for path, size in tqdm.tqdm(paths_for_deletion):
    #     log_event(json.dumps({"event": "delete", "path": path, "size": size,}))
    #
    #     break
    #
    #     # Uncomment the following line to actually run the deletions:
    #     # os.unlink(alto_path)
    #     # break
