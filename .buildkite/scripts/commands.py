# -*- encoding: utf-8

import subprocess
import sys


def check_call(cmd, exit_on_error=True):
    """
    A wrapped version of subprocess.check_call() that doesn't print a
    traceback if the command errors.
    """
    print("*** Running %r" % " ".join(cmd))
    try:
        return subprocess.check_output(cmd).decode("utf8").strip()
    except subprocess.CalledProcessError as err:
        if exit_on_error:
            sys.exit(err.returncode)
        pass


def make(*args):
    """Run a Make command, and check it completes successfully."""
    check_call(["make"] + list(args))


def git(*args, exit_on_error=True):
    """Run a Git command and return its output."""
    cmd = ["git"] + list(args)

    return check_call(cmd, exit_on_error=exit_on_error)
