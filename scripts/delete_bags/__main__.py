import datetime
import sys

import click
import humanize
from wellcome_storage_service import IngestNotFound

import azure
from deletions import record_deletion
from iam import (
    ADMIN_ROLE_ARN,
    create_aws_client_from_credentials,
    temporary_iam_credentials,
)
from iterators import chunked_iterable
from storage_service import (
    get_latest_version_of,
    get_locations_to_delete,
    lookup_ingest,
)
from s3 import list_objects_under, s3_sync


def hilight(s):
    return click.style(str(s), "green")


def abort(msg):
    sys.exit(click.style(msg, "red"))


def delete_ingest(
    *, api_name, ingest_id, locations, space, external_identifier, version, reason
):
    """
    Purge all the information about an ingest from the storage service.
    """
    click.echo("")

    # Now to give ourselves one last exit hatch, let's create a backup of the bag.
    # We'll copy it to the tmp/ prefix in wc-storage-infra.  Objects in this prefix
    # expire after 90 days, or they can be deleted immediately -- either way, we have
    # another backup if this script does something wrong.
    #
    # Note: we include the space and ingest ID in the backup key, and the
    # external identifier is available in the bag-info.txt.  The version is a
    # transient property of the storage service and can be safely discarded.
    backup_bucket = "wellcomecollection-storage-infra"
    backup_prefix = f"tmp/deleted_bags/{space}/{ingest_id}"

    click.echo(f"Creating a copy in {backup_bucket}...")

    s3_primary_location = locations[0]
    assert s3_primary_location["provider"] == "amazon-s3", s3_primary_location
    assert s3_primary_location["bucket"] in {
        "wellcomecollection-storage",
        "wellcomecollection-storage-staging",
    }, s3_primary_location

    # s3_sync(
    #     src_bucket=s3_primary_location["bucket"],
    #     src_prefix=s3_primary_location["prefix"],
    #     dst_bucket=backup_bucket,
    #     dst_prefix=backup_prefix,
    # )
    click.echo(
        f"Created a backup copy at {hilight(f's3://{backup_bucket}/{backup_prefix}')}"
    )

    # At this point we are committed to running the deletion, and it will run
    # to completion unless the user kills the script.
    #
    # Record the deletion event in DynamoDB, so we have an audit trail of all
    # our deleted bags.
    click.echo("")
    click.echo("Recording this deletion in DynamoDB...")
    # record_deletion(
    #     ingest_id=ingest_id,
    #     space=space,
    #     external_identifier=external_identifier,
    #     version=version,
    #     reason=reason
    # )

    # Now get AWS credentials to delete the S3 objects from the storage service.
    # Our standard storage-dev and storage-admin roles have a blanket Deny
    # on anything in the live storage service, so we'll have to create a one-off
    # user with the exact set of permissions we need.
    #
    # Creating a user with fine-grained permissions is an attempt to reduce the
    # risk of programming errors elsewhere screwing up the storage service -- if
    # the code gets overzealous and tries to delete extra stuff, the permissions
    # will protect us.
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": "",
                "Effect": "Allow",
                "Action": ["s3:DeleteObject"],
                "Resource": [f"arn:aws:s3:::{loc['bucket']}/{loc['prefix']}*"],
            }
            for loc in locations
            if loc["provider"] == "amazon-s3"
        ],
    }

    # with temporary_iam_credentials(
    #         admin_role_arn=ADMIN_ROLE_ARN, policy_document=policy_document
    #     ) as credentials:
    #         s3_client = create_aws_client_from_credentials("s3", credentials=credentials)
    #
    #         for loc in locations:
    #             if loc["provider"] != "amazon-s3":
    #                 continue
    #
    #             click.echo(f"Deleting objects in s3://{loc['bucket']}/{loc['prefix']}...")
    #
    #             # We can delete up to 1000 objects at once with the DeleteObjects API
    #             for batch in chunked_iterable(
    #                 list_objects_under(bucket=loc["bucket"], prefix=loc["prefix"]),
    #                 size=1000
    #             ):
    #                 s3_client.delete_objects(
    #                     Bucket=loc["bucket"],
    #                     Delete={
    #                         "Objects": [{"Key": s3_obj["key"]} for s3_obj in batch]
    #                     }
    #                 )

    # Next, go and clean up all the blobs in Azure.  To keep things simple,
    for loc in locations:
        if loc["provider"] != "azure-blob-storage":
            continue

        click.echo(f"Deleting objects in azure://{loc['bucket']}/{loc['prefix']}...")
        with azure.unlocked_container(container_name=loc["bucket"]):
            for blob_name in azure.list_blobs_under(
                container_name=loc["bucket"], prefix=loc["prefix"]
            ):
                azure.delete_blob(container_name=loc["bucket"], blob_name=blob_name)


def _check_is_latest_version(*, api_url, space, external_identifier, version):
    # It's possible for a later version of a bag to refer to an earlier version
    # of the bag in the fetch.txt.  Rather than exhaustively check for back-references
    # in future versions, we simply refuse to delete any bag which isn't the latest.
    #
    # This also avoids creating "holes" in the versioning.  If the latest version is N,
    # then you'd expect there also to be versions 1, 2, ..., N - 1 with no gaps.
    latest_version = get_latest_version_of(
        api_url=api_url, space=space, external_identifier=external_identifier
    )

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


@click.command()
@click.argument("ingest_id", required=True)
@click.option("--skip-az-login", default=False, is_flag=True)
def main(ingest_id, skip_az_login):
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

    date_str = date_created.strftime("%A, %d %B %Y @ %H:%M") + " (%s)" % delta
    click.echo(f"Date created: {hilight(date_str)}")

    click.echo("")
    # click.confirm("Are you sure you want to delete this bag?", abort=True)

    click.echo("")
    reason = "I'm testing the script for deleting bags"
    # reason = click.prompt("Why are you deleting this bag?")

    _check_is_latest_version(
        api_url=api_url,
        space=space,
        external_identifier=external_identifier,
        version=version,
    )

    # Now get the list of locations/prefixes we're going to delete from permanent
    # storage.  This is another place for the user to intervene if something seems
    # wrong.  It presents a prompt of the following form:
    #
    #       This bag is stored in 3 locations:
    #       - s3://wc-storage-staging/testing/test_bag/v132
    #       - s3://wc-storage-staging-replica-ireland/testing/test_bag/v132
    #       - azure://wc-storage-staging-replica-netherlands/testing/test_bag/v132
    #
    #       Does this look right? [y/N]: y
    #
    locations = get_locations_to_delete(
        api_url=api_url,
        space=space,
        external_identifier=external_identifier,
        version=version,
    )
    assert all(loc["prefix"].endswith(f"/{version}") for loc in locations)

    click.echo("")
    click.echo(
        f"This bag is stored in {hilight(len(locations))} location{'s' if len(locations) > 1 else ''}:"
    )
    for loc in locations:
        display_uri = loc["uri"] + "://" + loc["bucket"] + "/" + loc["prefix"]
        click.echo(f"- {hilight(display_uri)}")

    click.echo("")
    #    click.confirm("Does this look right?", abort=True)

    if not skip_az_login:
        click.echo("")
        click.echo("Logging in to Azure...")
        azure.az("login")

    delete_ingest(
        api_name=api_name,
        ingest_id=ingest_id,
        locations=locations,
        space=space,
        external_identifier=external_identifier,
        version=version,
        reason=reason,
    )


if __name__ == "__main__":

    main()
