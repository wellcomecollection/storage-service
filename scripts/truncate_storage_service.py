#!/usr/bin/env python
# -*- encoding: utf-8

import click

from common import get_aws_resource


def empty_dynamodb_table(name):
    dynamodb = get_aws_resource(
        "dynamodb", role_arn="arn:aws:iam::975596993436:role/storage-developer"
    )

    table = dynamodb.Table(name)

    key_names = [attribute["AttributeName"] for attribute in table.key_schema]

    while True:
        next_batch = table.scan()
        if not next_batch["Items"]:
            break

        with table.batch_writer() as batch:
            for item in next_batch["Items"]:
                batch.delete_item(Key={kf: item[kf] for kf in key_names})


def empty_s3_bucket(name):
    s3 = get_aws_resource(
        "s3", role_arn="arn:aws:iam::975596993436:role/storage-developer"
    )

    bucket = s3.Bucket(name)

    for s3_obj in bucket.objects.all():
        bucket.objects.delete()

    assert list(bucket.objects.all()) == []


def purge_sqs_queues(namespace):
    sqs = get_aws_resource(
        "sqs", role_arn="arn:aws:iam::975596993436:role/storage-developer"
    )

    sqs_client = sqs.meta.client

    for queue_url in sqs_client.list_queues(
            QueueNamePrefix=namespace)["QueueUrls"]:
        print("    - %s" % queue_url.split("/")[-1])
        sqs_client.purge_queue(QueueUrl=queue_url)


if __name__ == "__main__":
    service = click.prompt(
        "Which storage service would you like to truncate?",
        confirmation_prompt=True,
        type=click.Choice(["stage", "prod"]),
        show_choices=True,
    )

    click.confirm(
        "This will delete ALL ingests and ALL manifests. Are you sure?")
    click.confirm("Really sure?")
    click.confirm("Really really sure?")

    print("Okay, you're sure.")

    if service == "stage":
        namespace = "storage-staging"
        queue_namespace = "storage_staging"
    elif service == "prod":
        namespace = "storage"
        queue_namespace = "storage_prod"
        non_critical_namespace = "storage-prod"
    else:
        assert 0

    print("*** Deleting DynamoDB tables:")
    for table_name in [
        "%s-bag_id_lookup",
        "%s-ingests",
        "%s_replicas_table",
        "%s_versioner_versions_table",
        "vhs-%s-manifests",
    ]:
        print("    - %s" % (table_name % namespace))
        empty_dynamodb_table(table_name % namespace)

    for table_name in ["%s_replicator_lock_table", "%s_versioner_lock_table"]:
        print("    - %s" % (table_name % non_critical_namespace))
        empty_dynamodb_table(table_name % non_critical_namespace)

    print("*** Emptying S3 buckets:")
    for bucket_name in [
        "wellcomecollection-%s",
        "wellcomecollection-%s-replica-ireland",
        "wellcomecollection-vhs-%s-manifests",
    ]:
        print("    - %s" % (bucket_name % namespace))
        empty_s3_bucket(bucket_name % namespace)

    print("*** Purging SQS queues:")
    purge_sqs_queues(queue_namespace)
