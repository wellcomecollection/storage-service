#!/usr/bin/env python3
# -*- encoding: utf-8

from commands import git
from provider import current_branch, is_default_branch


if __name__ == "__main__":
    branch_name = current_branch()

    if is_default_branch():
        print(f"Successful completion on {branch_name}, tagging latest.")

        git("tag", "latest", "--force")
        git("push", "origin", "latest", "--force")

        print("Done.")
    else:
        print(f"Successful completion on {branch_name}, nothing to do.")
