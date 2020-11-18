#!/usr/bin/env python
"""
This script will delete a bag from the storage service.  Start by running:

    $ python3 ss_delete_bag.py --help

and follow the instructions from there.
"""

import sys

import click
import humanize
from wellcome_storage_service import IngestNotFound, RequestsOAuthStorageServiceClient

from helpers.storage_service import lookup_ingest


@click.command()
@click.argument("ingest_id", required=True)
def main(ingest_id):
    """
    Delete an ingest and the corresponding bag from the storage service.

    There is no "undo" button.  Please use with caution!

    I recommend testing with a test bag in the staging service before running
    this against prod, to check the script still works -- we don't use it very
    often, and it's possible something will have broken since the last time it
    was used.
    """
    try:
        api_name, api_url, ingest_data = lookup_ingest(ingest_id)
    except IngestNotFound:
        abort(f"Could not find {ingest_id} in either API!")

    space = ingest_data["space"]
    external_identifier = ingest_data["external_identifier"]
    version = ingest_data["version"]
    date_created = ingest_data["date_created"]

    # Prompt the user to confirm that yes, they really want to delete this bag.
    #
    # Note: the lack of a --confirm or --reason flag on this script is deliberate.
    # It adds a bit of friction to the process, so somebody can't accidentally
    # invoke this script and delete a whole pile of bags at once.
    _confirm_user_wants_to_delete_bag(
        api_name=api_name,
        space=space,
        external_identifier=external_identifier,
        version=version,
        date_created=date_created,
    )

    reason = _ask_reason_for_deleting_bag(
        space=space, external_identifier=external_identifier, version=version
    )

    storage_client = RequestsOAuthStorageServiceClient.from_path(api_url=api_url)

    _confirm_is_latest_version_of_bag(
        storage_client,
        space=space,
        external_identifier=external_identifier,
        version=version,
    )


def hilight(s):
    return click.style(str(s), "green")


def abort(msg):
    sys.exit(click.style(msg, "red"))


def _confirm_user_wants_to_delete_bag(
    api_name, space, external_identifier, version, date_created
):
    """
    Show the user some information about the bag, and check this is really
    the bag they want to delete.

    It presents a prompt of the following form:

          This is the bag you are about to delete:
          Space:        testing
          External ID:  test_bag
          Version:      v132
          Date created: Friday, 13 November 2020 @ 11:10 (2 days ago)

          Are you sure you want to delete this bag? [y/N]: y

    """
    delta = humanize.naturaltime(date_created)

    click.echo("")
    click.echo("This is the bag you are about to delete:")
    click.echo(f"Environment:  {hilight(api_name)}")
    click.echo(f"Space:        {hilight(space)}")
    click.echo(f"External ID:  {hilight(external_identifier)}")
    click.echo(f"Version:      {hilight(version)}")

    date_str = date_created.strftime("%A, %d %B %Y @ %H:%M") + " (%s)" % delta
    click.echo(f"Date created: {hilight(date_str)}")

    click.echo("")
    click.confirm("Are you sure you want to delete this bag?", abort=True)


def _ask_reason_for_deleting_bag(*, space, external_identifier, version):
    """
    Ask the user why they want to delete this abg.  This reason will be recorded
    for audit purposes.
    """
    click.echo("")
    bag_id = f"{space}/{external_identifier}/{version}"
    return click.prompt(f"Why are you deleting {hilight(bag_id)}?")


def _confirm_is_latest_version_of_bag(
    storage_client, *, space, external_identifier, version
):
    """
    It's possible for a later version of a bag to refer to an earlier version
    of the bag in the fetch.txt.  Rather than exhaustively check for back-references
    in future versions, we simply refuse to delete any bag which isn't the latest.

    This also avoids creating "holes" in the versioning.  If the latest version is N,
    then you'd expect there also to be versions 1, 2, ..., N - 1 with no gaps.

    """
    latest_bag = storage_client.get_bag(
        space=space, external_identifier=external_identifier
    )
    latest_version = latest_bag["version"]

    bag_id = f"{space}/{external_identifier}"

    click.echo("")

    version_i = int(version[1:])
    latest_version_i = int(latest_version[1:])

    click.echo("Checking this is the latest version...")
    if version_i == latest_version_i:
        click.echo(f"{version} is the latest version of {bag_id}")
    elif version_i < latest_version_i:
        abort(
            f"The latest version of {bag_id} is {latest_version}, "
            f"which is newer than {version}"
        )
    elif version_i > latest_version_i:
        abort(
            f"Something is wrong -- the bags API only knows about {latest_version}, "
            f"which is older than {version}"
        )


if __name__ == "__main__":
    main()
