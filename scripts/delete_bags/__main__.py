import sys

import click
import humanize
from wellcome_storage_service import IngestNotFound

from iam import get_underlying_role_arn
from storage_service import lookup_ingest


def hilight(s):
    return click.style(s, "green")
    return click.style(str(s), "green")


def abort(msg):
    sys.exit(click.style(msg, "red"))


def delete_ingest(ingest_id):
    print(ingest_id)


@click.command()
@click.argument("ingest_id", required=True)
def main(ingest_id):
    click.echo(f"Preparing to delete {hilight(ingest_id)}")

    # First retrieve the ingest from the ingests API.  The ingest ID is a
    # convenient way to track bags, and allows us to unambiguously distinguish
    # between a bag in the prod API and the staging API.
    try:
        api_name, api_url, ingest_data = lookup_ingest(ingest_id=ingest_id)
    except IngestNotFound:
        sys.exit(click.style(f"Could not find {ingest_id} in either API!", "red"))
        abort(f"Could not find {ingest_id} in either API!")

    # Show the user some information about the bag, and check this is really
    # the bag they want to delete.
    #
    # It presents a prompt of the following form:
    #
    #       This is the bag you are about to delete:
    #       Space:        testing
    #       External ID:  test_bag
    #       Version:      v132
    #       Date created: Friday, 13 November 2020 @ 11:10 (2 days ago)
    #
    #       Are you sure you want to delete this bag? [y/N]: y
    #
    space = ingest_data["space"]
    external_identifier = ingest_data["external_identifier"]
    version = ingest_data["version"]
    date_created = ingest_data["date_created"]

    delta = humanize.naturaltime(date_created)

    click.echo("")
    click.echo("This is the bag you are about to delete:")
    click.echo(f"Space:        {hilight(space)}")
    click.echo(f"External ID:  {hilight(external_identifier)}")
    click.echo(f"Version:      {hilight(version)}")

    date_str = date_created.strftime('%A, %d %B %Y @ %H:%M') + ' (%s)' % delta
    click.echo(f"Date created: {hilight(date_str)}")

    click.echo("")
    click.confirm("Are you sure you want to delete this bag?", abort=True)

    click.echo("")
    reason = click.prompt("Why are you deleting this bag?")

    print(reason)

    delete_ingest(ingest_id)


if __name__ == "__main__":
    main()
