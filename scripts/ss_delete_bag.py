#!/usr/bin/env python
"""
This script will delete a bag from the storage service.  Start by running:

    $ python3 ss_delete_bag.py --help

and follow the instructions from there.
"""

import collections
import datetime
import json
import os
import sys
import textwrap

import boto3
import click
from elasticsearch.exceptions import NotFoundError as ElasticNotFoundError
import humanize
from wellcome_storage_service import staging_client, prod_client

from helpers import azure, dynamo, s3
from helpers.iam import (
    ACCOUNT_ID,
    ADMIN_ROLE_ARN,
    DEV_ROLE_ARN,
    READ_ONLY_ROLE_ARN,
    create_aws_client_from_credentials,
    create_aws_client_from_role_arn,
    create_dynamo_client_from_role_arn,
    get_underlying_role_arn,
    temporary_iam_credentials,
)
from helpers.reporting import get_reporting_client
from helpers.s3 import delete_s3_prefix
from helpers.storage_service import lookup_ingest_id


@click.command()
@click.option("--environment", required=True, type=click.Choice(["prod", "staging"]))
@click.option("--space", required=True)
@click.option("--external-identifier", required=True)
@click.option("--version", required=True)
@click.option("--skip-azure-login", is_flag=True, default=False)
def main(environment, space, external_identifier, version, skip_azure_login):
    """
    Delete a bag and the corresponding ingest from the storage service.

    There is no "undo" button.  Please use with caution!

    I recommend testing with a test bag in the staging service before running
    this against prod, to check the script still works -- we don't use it very
    often, and it's possible something will have broken since the last time it
    was used.
    """
    ingest_id = lookup_ingest_id(environment, space, external_identifier, version)

    if environment == "staging":
        storage_client = staging_client()
    elif environment == "prod":
        storage_client = prod_client()
    else:
        sys.exit(f"Unrecognised environment: {environment}")

    ingest = storage_client.get_ingest(ingest_id)
    date_created = datetime.datetime.strptime(
        ingest["createdDate"], "%Y-%m-%dT%H:%M:%S.%fZ"
    )

    # Prompt the user to confirm that yes, they really want to delete this bag.
    #
    # Note: the lack of a --confirm or --reason flag on this script is deliberate.
    # It adds a bit of friction to the process, so somebody can't accidentally
    # invoke this script and delete a whole pile of bags at once.
    _confirm_user_wants_to_delete_bag(
        api_name=environment,
        space=space,
        external_identifier=external_identifier,
        version=version,
        date_created=date_created,
        ingest_id=ingest_id,
    )

    reason = _ask_reason_for_deleting_bag(
        space=space, external_identifier=external_identifier, version=version
    )

    # Do various other checks that this bag is correctly formed before we go
    # ahead and start deleting stuff.  The deletion should be as close to atomic
    # as possible -- we shouldn't get halfway through deleting the bag and then
    # discover something is wrong; we should catch problems upfront.
    _confirm_is_latest_version_of_bag(
        storage_client,
        space=space,
        external_identifier=external_identifier,
        version=version,
    )

    bag = storage_client.get_bag(
        space=space, external_identifier=external_identifier, version=version
    )

    # We only want to delete files that were newly introduced in this version --
    # we shouldn't delete files fetch.txt'd from a prior version.
    files_to_delete = [
        f
        for f in bag["manifest"]["files"] + bag["tagManifest"]["files"]
        if f["path"].startswith(f"{version}/")
    ]

    locations = _confirm_user_wants_to_delete_locations(bag)
    assert all(
        loc["prefix"].endswith(f"/{version}")
        for loc in locations["s3"] + [locations["azure"]]
    )

    dynamo_client = create_dynamo_client_from_role_arn(role_arn=READ_ONLY_ROLE_ARN)

    items_to_delete = list(
        _get_dynamodb_items_to_delete(
            dynamo_client,
            environment=environment,
            ingest_id=ingest_id,
            space=space,
            external_identifier=external_identifier,
            version=version,
            files_to_delete=files_to_delete,
            azure_location=locations["azure"],
        )
    )

    click.echo("")
    click.echo(
        "Creating a temporary backup copy of the bag in s3://wellcomecollection-storage-infra..."
    )
    s3.copy_s3_prefix(
        create_aws_client_from_role_arn("s3", role_arn=DEV_ROLE_ARN),
        src_bucket=locations["s3"][0]["bucket"],
        src_prefix=locations["s3"][0]["prefix"],
        dst_bucket="wellcomecollection-storage-infra",
        # Note: we drop the version and external identifier.  The version is
        # a property of the storage sevrice; the external identifier is part of
        # the bag-info.txt.
        dst_prefix=f"tmp/deleted_bags/{space}/{ingest_id}",
    )

    if not skip_azure_login:
        click.echo("")
        click.echo("Logging in to Azure...")
        azure.az("login")

    # At this point, we've checked everything -- we're good to go!  Let's
    # make a record of the deletion we're about to do.
    dynamo_client = create_dynamo_client_from_role_arn(role_arn=DEV_ROLE_ARN)
    _record_deletion(
        dynamo_client,
        environment=environment,
        ingest_id=ingest_id,
        space=space,
        external_identifier=external_identifier,
        version=version,
        reason=reason,
    )

    # And now start deleting stuff!
    _delete_reporting_cluster_entries(
        environment=environment,
        ingest_id=ingest_id,
        space=space,
        external_identifier=external_identifier,
        version=version,
        s3_location=locations["s3"][0],
        files_to_delete=files_to_delete,
    )

    _delete_s3_objects(s3_locations=locations["s3"])
    _delete_azure_blobs(azure_location=locations["azure"])

    _delete_dynamodb_items(items_to_delete)

    click.echo("")
    click.echo(
        click.style(
            textwrap.dedent(
                f"""
    This bag has been deleted.

    A temporary copy has been saved in s3://wellcomecollection-storage-infra/tmp/deleted-bags/{ingest_id},
    but this will only be kept for 30 days.
    """
            ).strip(),
            "red",
        )
    )


def hilight(s):
    return click.style(str(s), "green")


def abort(msg):
    sys.exit(click.style(msg, "red"))


def _confirm_user_wants_to_delete_bag(
    api_name, space, external_identifier, version, date_created, ingest_id
):
    """
    Show the user some information about the bag, and check this is really
    the bag they want to delete.

    It presents a prompt of the following form:

        This is the bag you are about to delete:
        Environment:  prod
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
    click.echo(f"Ingest ID:    {hilight(ingest_id)}")

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


def _confirm_user_wants_to_delete_locations(bag):
    """
    Now get the list of locations/prefixes we're going to delete from permanent
    storage.  This is another place for the user to intervene if something seems
    wrong.  It presents a prompt of the following form:

          This bag is stored in 3 locations:
          - s3://wc-storage-staging/testing/test_bag/v132
          - s3://wc-storage-staging-replica-ireland/testing/test_bag/v132
          - azure://wc-storage-staging-replica-netherlands/testing/test_bag/v132

          Does this look right? [y/N]: y

    """
    click.echo("")
    click.echo("Checking the locations to delete...")

    location = bag["location"]
    if location["provider"]["id"] != "amazon-s3":
        abort(
            "Something is wrong: the primary location of the bag isn't S3:\n"
            + json.dumps(location, indent=2, sort_keys=True)
        )

    replicas = bag["replicaLocations"]
    replica_providers = {r["provider"]["id"] for r in replicas}

    if len(replicas) != 2 or replica_providers != {"amazon-s3", "azure-blob-storage"}:
        abort(
            "This script only knows how to deal with exactly two replicas: one S3, "
            f"one Azure.  This bag has {len(replicas)}:\n"
            + json.dumps(replicas, indent=2, sort_keys=True)
        )

    s3_replica = next(r for r in replicas if r["provider"]["id"] == "amazon-s3")
    azure_replica = next(
        r for r in replicas if r["provider"]["id"] == "azure-blob-storage"
    )

    # All the paths should be the same in each bucket/container; if they're not,
    # something is dodgy about this bag.
    prefixes = {location["path"], s3_replica["path"], azure_replica["path"]}
    if len(prefixes) != 1:
        abort(
            "All the replicas should have the same prefix.  This bag has different "
            "prefixes in different buckets/containers:\n"
            + json.dumps(
                {"location": location, "replicas": replicas}, indent=2, sort_keys=True
            )
        )

    # The 'prefix' returned in the locations block on a bag refers to *all* versions
    # of a bag, but when doing deletions we only want to delete a single version.
    common_prefix = os.path.join(prefixes.pop(), bag["version"])

    try:
        azure_account = {
            "wellcomecollection-storage-staging-replica-netherlands": "wecostoragestage",
            "wellcomecollection-storage-replica-netherlands": "wecostorageprod",
        }[azure_replica["bucket"]]
    except KeyError:
        abort(
            "Unrecognised Azure container in the Azure replica location:\n"
            + json.dumps(azure_replica, indent=2, sort_keys=True)
        )

    locations = {
        "s3": [
            {"bucket": location["bucket"], "prefix": common_prefix},
            {"bucket": s3_replica["bucket"], "prefix": common_prefix},
        ],
        "azure": {
            "account": azure_account,
            "container": azure_replica["bucket"],
            "prefix": common_prefix,
        },
    }

    loc_uris = [f"s3://{loc['bucket']}/{loc['prefix']}" for loc in locations["s3"]] + [
        f"azure://{locations['azure']['container']}/{locations['azure']['prefix']}"
    ]

    click.echo(f"This bag is stored in {hilight(len(loc_uris))} locations:")
    for loc in loc_uris:
        click.echo(f"- {hilight(loc)}")
    click.echo("")
    click.confirm("Does this look right?", abort=True)

    return locations


DynamoItem = collections.namedtuple("DynamoItem", ["table", "key"])


def _get_dynamodb_items_to_delete(
    dynamo_client,
    *,
    environment,
    ingest_id,
    space,
    external_identifier,
    version,
    files_to_delete,
    azure_location,
):
    """
    Returns all the DynamoDB items that should be deleted, as a list of tuples

        (table, key)

    """
    table_names = set(dynamo.list_dynamo_tables(dynamo_client))
    table_prefix = "storage" if environment == "prod" else f"storage-{environment}"

    # All the tags from the Azure verifier table.  There should be one item
    # per file.  Note: these will eventually be removed when Azure get supported
    # for index tags directly on blobs.
    #
    # This information can be reconstructed by running the verifier over the
    # Azure replica.
    assert all(
        f["path"].startswith(f"{version}/") for f in files_to_delete
    ), files_to_delete
    assert azure_location["prefix"].endswith(f"/{version}")

    azure_table_name = f"storage-{environment}_azure_verifier_tags"
    assert azure_table_name in table_names

    for f in files_to_delete:
        azure_uri = f"azure://{azure_location['container']}/{azure_location['prefix']}/{f['name']}"
        yield DynamoItem(table=azure_table_name, key={"id": azure_uri})

    # The replicas table.  We delete this next -- this information can be
    # reconstructed by running the verifier over the replicas.
    replicas_table_name = f"{table_prefix}_replicas_table"
    assert replicas_table_name in table_names

    yield DynamoItem(
        table=replicas_table_name,
        key={"id": f"{space}/{external_identifier}/{version}"},
    )

    # The versions table.  This information can be reconstructed; at this point
    # most references to this version of the bag have been deleted.
    versions_table_name = f"{table_prefix}_versioner_versions_table"
    assert versions_table_name in table_names

    yield DynamoItem(
        table=versions_table_name,
        key={"id": f"{space}/{external_identifier}", "version": int(version[1:])},
    )

    # The storage manifest table.
    manifests_table_name = dynamo.find_manifests_dynamo_table(
        dynamo_client, table_prefix=table_prefix
    )
    assert manifests_table_name in table_names

    yield DynamoItem(
        table=manifests_table_name,
        key={"id": f"{space}/{external_identifier}", "version": int(version[1:])},
    )

    # The ingests table
    ingests_table_name = f"{table_prefix}-ingests"
    yield DynamoItem(table=ingests_table_name, key={"id": ingest_id})


def _record_deletion(
    dynamo_client,
    *,
    environment,
    ingest_id,
    space,
    external_identifier,
    version,
    reason,
):
    """
    Create a record of this deletion event in DynamoDB.
    """
    event = {
        "requested_by": get_underlying_role_arn(),
        "deleted_at": datetime.datetime.now().isoformat(),
        "reason": reason,
    }

    dynamo_client.update_item(
        TableName="deleted_bags",
        Key={"ingest_id": ingest_id},
        UpdateExpression="""
             SET
             #space = :space,
             #externalIdentifier = :externalIdentifier,
             #version = :version,
             #environment = :environment,
             #events = list_append(if_not_exists(#events, :empty_list), :event)
        """,
        ExpressionAttributeNames={
            "#environment": "environment",
            "#space": "space",
            "#externalIdentifier": "externalIdentifier",
            "#version": "version",
            "#events": "events",
        },
        ExpressionAttributeValues={
            ":environment": environment,
            ":space": space,
            ":externalIdentifier": external_identifier,
            ":version": version,
            ":event": [event],
            ":empty_list": [],
        },
    )


def _delete_reporting_cluster_entries(
    *,
    environment,
    ingest_id,
    space,
    external_identifier,
    version,
    s3_location,
    files_to_delete,
):
    click.echo("")
    click.echo("Deleting entries in the reporting cluster...")

    index_pattern = {"staging": "storage_stage_{doc}", "prod": "storage_{doc}"}[
        environment
    ]

    secrets_client = create_aws_client_from_role_arn(
        "secretsmanager", role_arn=ADMIN_ROLE_ARN
    )

    # Delete the ingest.
    #
    # In particular, delete the entry in the ingests index with this ingest ID.
    #
    click.echo("Deleting this ingest from the ingests index...")
    ingests_reporting_client = get_reporting_client(
        secrets_client, environment=environment, app_name="ingests"
    )
    try:
        ingests_reporting_client.delete(
            index=index_pattern.format(doc="ingests"), id=ingest_id
        )
    except ElasticNotFoundError:
        pass

    # Delete the bag.
    #
    # In particular, delete the entry in the bags index with the ID
    # {space}/{external_identifier}
    #
    # Note: this assumes a single version of each bag will be indexed.  If we change
    # this, we'll need to change this code to match.
    #
    click.echo("Deleting this bag from the bags index...")
    bags_reporting_client = get_reporting_client(
        secrets_client, environment=environment, app_name="bags"
    )
    try:
        bags_reporting_client.delete(
            index=index_pattern.format(doc="bags"), id=f"{space}/{external_identifier}"
        )
    except ElasticNotFoundError:
        pass

    # If this wasn't v1 of the bag, trigger a reindex so we get the bag in the
    # reporting cluster.
    if version != "v1":
        version_i = int(version[1:])

        reindexer_topic_arn = f"arn:aws:sns:eu-west-1:{ACCOUNT_ID}:storage-{environment}_bag_reindexer_output"
        sns_client = create_aws_client_from_role_arn("sns", role_arn=DEV_ROLE_ARN)

        payload = {
            "space": space,
            "externalIdentifier": external_identifier,
            "version": f"v{version_i - 1}",
            "type": "RegisteredBagNotification",
        }

        sns_client.publish(
            TopicArn=reindexer_topic_arn,
            Subject=f"Sent by {__file__}",
            Message=json.dumps(payload),
        )

    # Delete all the files.
    #
    # In particular, delete all files whose key matches the common prefix
    # in all our locations.
    #
    click.echo("Deleting all the matching files from the files index...")
    assert all(f["path"].startswith(f"{version}/") for f in files_to_delete)
    files_reporting_client = get_reporting_client(
        secrets_client, environment=environment, app_name="files"
    )

    for f in files_to_delete:
        try:
            files_reporting_client.delete(
                index=index_pattern.format(doc="files"),
                id=f"{s3_location['prefix']}/{f['name']}",
            )
        except ElasticNotFoundError:
            pass


def _delete_s3_objects(*, s3_locations):
    click.echo("")
    click.echo("Deleting objects from S3...")
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
                "Sid": f"DeleteInBucket{i}",
                "Effect": "Allow",
                "Action": ["s3:DeleteObject"],
                "Resource": [f"arn:aws:s3:::{loc['bucket']}/{loc['prefix']}*"],
            }
            for i, loc in enumerate(s3_locations)
        ]
        + [
            {
                "Sid": "ListAll",
                "Effect": "Allow",
                "Action": ["s3:List*"],
                "Resource": ["*"],
            }
        ],
    }

    with temporary_iam_credentials(
        admin_role_arn=ADMIN_ROLE_ARN, policy_document=policy_document
    ) as credentials:
        s3_list_client = create_aws_client_from_role_arn(
            "s3", role_arn=READ_ONLY_ROLE_ARN
        )
        s3_delete_client = create_aws_client_from_credentials(
            "s3", credentials=credentials
        )

        for loc in s3_locations:
            click.echo(f"Deleting objects in s3://{loc['bucket']}/{loc['prefix']}")
            delete_s3_prefix(
                s3_list_client=s3_list_client,
                s3_delete_client=s3_delete_client,
                bucket=loc["bucket"],
                prefix=loc["prefix"],
            )


def _delete_azure_blobs(*, azure_location):
    click.echo("")
    click.echo("Deleting blobs from Azure...")

    click.echo(
        f"Deleting blobs in azure://{azure_location['container']}/{azure_location['prefix']}"
    )

    with azure.unlocked_azure_container(
        account=azure_location["account"], container=azure_location["container"]
    ):
        azure.delete_azure_prefix(
            account=azure_location["account"],
            container=azure_location["container"],
            prefix=azure_location["prefix"],
        )


def _delete_dynamodb_items(items_to_delete):
    click.echo("")
    click.echo("Deleting items from DynamoDB...")

    # Now get AWS credentials to delete the DynamoDB items from the storage service.

    # The Azure verifier tags table may have arbitrarily many items, and the
    # entire table can be reconstructed, so it doesn't have delete protections.
    azure_verifier_tags = [
        it for it in items_to_delete if it.table.endswith("_azure_verifier_tags")
    ]

    remaining_items = [
        it for it in items_to_delete if not it.table.endswith("_azure_verifier_tags")
    ]

    assert len(azure_verifier_tags) + len(remaining_items) == len(items_to_delete)

    dynamo_client = create_dynamo_client_from_role_arn(role_arn=DEV_ROLE_ARN)

    # The table should be unique
    assert len({it.table for it in azure_verifier_tags}) == 1

    # there should always be some files, so this [0] will never throw an IndexError
    azure_verifier_tags_table = azure_verifier_tags[0].table

    dynamo.bulk_delete_dynamo_items(
        dynamo_client,
        table_name=azure_verifier_tags_table,
        keys=[it.key for it in azure_verifier_tags],
    )

    # Now go through the remaining items.  There should only be a few; enough
    # that we can delete them all in one batch.
    policy_document = {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Sid": f"DeleteItem{i}",
                "Effect": "Allow",
                "Action": ["dynamodb:DeleteItem"],
                "Resource": [
                    f"arn:aws:dynamodb:eu-west-1:{ACCOUNT_ID}:table/{item.table}"
                ],
                "Condition": {
                    "ForAllValues:StringEquals": {
                        "dynamodb:LeadingKeys": [item.key["id"]]
                    }
                },
            }
            for i, item in enumerate(remaining_items)
        ],
    }

    with temporary_iam_credentials(
        admin_role_arn=ADMIN_ROLE_ARN, policy_document=policy_document
    ) as credentials:
        dynamo_client = boto3.resource(
            "dynamodb",
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        ).meta.client

        for item in remaining_items:
            dynamo_client.delete_item(TableName=item.table, Key=item.key)


if __name__ == "__main__":
    main()
