#!/usr/bin/env python
"""
This script will delete a bag from the storage service.  Start by running:

    $ python3 ss_delete_bag.py --help

and follow the instructions from there.
"""

import click


@click.command()
def main():
    """
    Delete an ingest and the corresponding bag from the storage service.

    There is no "undo" button.  Please use with caution!

    I recommend testing with a test bag in the staging service before running
    this against prod, to check the script still works -- we don't use it very
    often, and it's possible something will have broken since the last time it
    was used.
    """
    pass


if __name__ == "__main__":
    main()
