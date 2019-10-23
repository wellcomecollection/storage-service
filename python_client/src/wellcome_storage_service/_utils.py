# -*- encoding: utf-8


import errno
import os


def mkdir_p(path):
    """
    Create a directory if it does not already exist.

    This can be replaced by os.makedirs(..., exist_ok=True) or
    pathlib.Path(...).makedirs(exist_ok=True) in Python 3.

    From https://stackoverflow.com/a/600612/1558022
    """
    try:
        os.makedirs(path)
    except OSError as exc:
        if exc.errno == errno.EEXIST and os.path.isdir(path):
            pass
        else:
            raise
