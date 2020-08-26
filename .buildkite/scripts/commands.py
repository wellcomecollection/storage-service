# -*- encoding: utf-8

import subprocess
import sys


def _subprocess_run(cmd, exit_on_error=True):
    print("*** Running %r" % " ".join(cmd))

    output = []
    pipe = subprocess.Popen(
        cmd, encoding="utf8", stdout=subprocess.PIPE, stderr=subprocess.STDOUT
    )

    # Await command completion and print lines as they come in
    for stdout_line in iter(pipe.stdout.readline, ""):
        print(stdout_line, end="")
        output.append(stdout_line)

    # Extract results
    pipe.communicate()
    return_code = pipe.returncode

    if return_code != 0 and exit_on_error:
        sys.exit(return_code)

    return "\n".join(output).strip()


def make(*args):
    """Run a Make command, and check it completes successfully."""
    _subprocess_run(["make"] + list(args))


def git(*args, exit_on_error=True):
    """Run a Git command and return its output."""
    cmd = ["git"] + list(args)

    return _subprocess_run(cmd, exit_on_error=exit_on_error)
