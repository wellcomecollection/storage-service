#!/usr/bin/env python

import datetime
import glob
import json
import subprocess

import humanize
import termcolor


if __name__ == "__main__":
    total_deleted = 1339218
    total_size = 84395734645

    processes = int(
        subprocess.check_output(
            "ps -eaf | grep python | grep cleanup_alto | wc -l", shell=True
        )
    ) - 1

    for log_path in glob.iglob("*.log"):
        for line in open(log_path):
            try:
                data = json.loads(line)
            except ValueError:
                continue

            total_deleted += 1
            total_size += data["size"]

    print(
        "Running %s processes; deleted %s (%s bytes) in %s files @ %s"
        % (
            termcolor.colored(processes, "green"),
            termcolor.colored(humanize.naturalsize(total_size), "green"),
            humanize.intcomma(total_size),
            termcolor.colored(total_deleted, "green"),
            datetime.datetime.now().isoformat(),
        )
    )
