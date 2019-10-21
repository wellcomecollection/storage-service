#!/usr/bin/env python
# -*- encoding: utf-8
"""
Download the contents of a bag.  Usage:

    python ss_download_bag.py <SPACE> <EXTERNAL_IDENTIFIER> [<VERSION>]

"""

import sys
import tempfile

import click
from wellcome_storage_service import download_bag

from common import get_logger
from ss_get_bag import lookup_bag


logger = get_logger(__name__)


def confirm_size(storage_manifest):
    all_files = storage_manifest["manifest"]["files"] + storage_manifest["tagManifest"]["files"]
    total_size = sum(f["size"] for f in all_files)

    # If the size is >100MB, double-check before initiating the download.
    if total_size > 1000 * 1000 * 1000:
        import humanize

        click.confirm(
            f"Total size of bag is {humanize.naturalsize(total_size)}.  Download?"
        )


if __name__ == "__main__":
    try:
        space = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <SPACE> <EXTERNAL_IDENTIFIER> [<VERSION>]")

    try:
        external_identifier = sys.argv[2]
    except IndexError:
        sys.exit(f"Usage: {__file__} <SPACE> <EXTERNAL_IDENTIFIER> [<VERSION>]")

    try:
        version = sys.argv[3]
    except IndexError:
        version = None

    storage_manifest = lookup_bag(space, external_identifier, version)

    confirm_size(storage_manifest)

    out_dir = tempfile.mkdtemp()

    download_bag(storage_manifest=storage_manifest, out_dir=out_dir)

    print(out_dir)
