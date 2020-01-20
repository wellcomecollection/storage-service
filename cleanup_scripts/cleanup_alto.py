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

        for f in filenames:
            if not f.endswith(".xml"):
                continue

            yield (f, os.path.join(dirpath, f))

        # if os.path.listdir(dirpath) == []:
        #     shutil.rmtree(dirpath)


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
def get_files(b_number):
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


def log_event(s):
    with open(f"alto_cleanup_{NOW}.log", "a") as out_file:
        out_file.write(s.rstrip() + "\n")


if __name__ == "__main__":
    root = "/Volumes/LIB_WDL_DDS/LIB_WDL_DDS_METS/"

    # Trying to look for the b number in an ALTO filename, e.g.
    #
    #     b21020000_0001.xml
    #
    B_NUMBER_RE = re.compile(r"^(?P<b_number>b[0-9]{7}[0-9x])")

    for alto_name, alto_path in tqdm.tqdm(get_alto_paths(root)):
        try:
            b_number = B_NUMBER_RE.match(alto_name).group("b_number")
        except AttributeError:
            warn(f"Unable to identify b number in {alto_path!r}")
            continue

        files = get_files(b_number)

        try:
            matching_file = files[alto_name]
        except KeyError:
            warn(f"Unable to find file in storage manifest for {alto_path!r}")
            continue

        if size(alto_path) != matching_file["size"]:
            warn(
                f"Sizes don't match:\nPath       = %s\nMETS share = %10d\nStorage    = %10d"
                % (alto_path, size(alto_path), matching_file["size"])
            )
            continue

        if checksum(alto_path) != matching_file["checksum"]:
            warn(
                f"Checksums don't match:\nPath       = %s\nMETS share = %s\nStorage    = %s\n"
                % (alto_path, checksum(alto_path), matching_file["checksum"])
            )
            continue

        log_event(
            json.dumps({"event": "delete", "path": alto_path, "size": size(alto_path)})
        )

        # Uncomment the following line to actually run the deletions:
        # os.unlink(alto_path)
        # break
