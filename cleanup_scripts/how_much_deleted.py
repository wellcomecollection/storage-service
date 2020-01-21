#!/usr/bin/env python

import glob
import json

import humanize
import termcolor


if __name__ == "__main__":
    total_deleted = 0
    total_size = 0

    for log_path in glob.iglob("*.log"):
        for line in open(log_path):
            try:
                data = json.loads(line)
            except ValueError:
                continue

            total_deleted += 1
            total_size += data["size"]

    print(
        "Deleted %s (%s bytes) in %s files"
        % (
            termcolor.colored(humanize.naturalsize(total_size), "green"),
            humanize.intcomma(total_size),
            termcolor.colored(total_deleted, "green"),
        )
    )
