# -*- encoding: utf-8

import subprocess
import sys


def check_call(cmd):
    """
    A wrapped version of subprocess.check_call() that doesn't print a
    traceback if the command errors.
    """
    print("*** Running %r" % " ".join(cmd))
    try:
        return subprocess.check_output(cmd).decode("utf8").strip()
    except subprocess.CalledProcessError as err:
        sys.exit(err.returncode)


def make(*args):
    """Run a Make command, and check it completes successfully."""
    check_call(["make"] + list(args))


def git(*args):
    """Run a Git command and return its output."""
    cmd = ["git"] + list(args)

    return check_call(cmd)
