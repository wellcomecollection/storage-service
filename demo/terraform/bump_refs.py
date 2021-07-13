#!/usr/bin/env python
"""
This script bumps all the module references in the demo stack to
a given Git commit, then bumps the README and example instance to match.

This is meant to make it easier to keep the demo working/up-to-date with
changes in the storage service.
"""

import os
import re
import subprocess
import sys


def get_file_paths_under(root="."):
    """Generates the paths to every file under ``root``."""
    if not os.path.isdir(root):
        raise ValueError(f"Cannot find files under non-existent directory: {root!r}")

    for dirpath, _, filenames in os.walk(root):
        for f in filenames:
            if os.path.isfile(os.path.join(dirpath, f)):
                yield os.path.join(dirpath, f)


def update_commit_id(line, commit_id):
    return re.sub(
        r'source = "github\.com/wellcomecollection/storage-service\.git//(?P<directory>[^\?]+)\?ref=[a-f0-9]+"',
        f'source = "github.com/wellcomecollection/storage-service.git//\g<directory>?ref={commit_id}"',
        line
    )


def update_file(path, commit_id):
    old_lines = list(open(path))
    new_lines = [update_commit_id(line, commit_id) for line in old_lines]

    if old_lines != new_lines:
        with open(path, "w") as outfile:
            outfile.write("".join(new_lines))
        subprocess.check_call(["git", "add", path])


if __name__ == '__main__':
    try:
        commit_id = sys.argv[1]
    except IndexError:
        sys.exit(f"Usage: {__file__} <COMMIT_ID>")

    for path in get_file_paths_under("demo_stack"):
        if path.endswith(".tf"):
            update_file(path, commit_id)

    subprocess.check_call(["git", "commit", "-m", f"Bump module references in the demo stack to {commit_id}"])

    current_commit = subprocess.check_output(["git", "rev-parse", "HEAD"]).strip().decode("utf8")[:7]

    for f in ("README.md", "main.tf"):
        update_file(f, current_commit)
        subprocess.check_call(["git", "add", f])

    subprocess.check_call(["git", "commit", "-m", f"Bump module references in the demo stack README to {current_commit}"])
