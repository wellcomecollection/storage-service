#!/usr/bin/env python

import boto3


def get_client(resource, *, port):
    return boto3.client(
        resource,
        endpoint_url=f"http://localhost:{port}",
        aws_access_key_id="123",
        aws_secret_access_key="abc",
    )


def create_sns_topic(sns_client, *, name):
    """
    Creates an SNS topic, returns the topic ARN.
    """
    resp = sns_client.create_topic(Name="bag_unpacker_input")

    return resp["TopicArn"]


def create_sqs_queue(sqs_client, *, name):
    """
    Creates an SQS queue, returns the queue URL.
    """
    resp = sqs_client.create_queue(QueueName="bag_unpacker_input")

    return resp["QueueUrl"]


def create_topic_queue_pair(sns_client, sqs_client, name):
    topic_arn = create_sns_topic(sns_client, name="bag_unpacker_input")

    queue_url = create_sqs_queue(sqs_client, name="bag_unpacker_input")

    sns_client.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_url)

    return (topic_arn, queue_url)


def underlined(s):
    return "\033[4m" + s + "\033[0m"


if __name__ == "__main__":
    dynamodb_client = get_client("dynamodb", port=4569)
    sns_client = get_client("sns", port=4575)
    sqs_client = get_client("sqs", port=4576)

    bag_unpacker_topic_arn, bag_unpacker_queue_url = create_topic_queue_pair(
        sns_client=sns_client, sqs_client=sqs_client, name="bag_unpacker_input"
    )

    print(f"bag unpacker topic arn = {underlined(bag_unpacker_topic_arn)}")
    print(f"bag unpacker queue url = {underlined(bag_unpacker_queue_url)}")

    ingests_topic_arn, ingests_queue_url = create_topic_queue_pair(
        sns_client=sns_client, sqs_client=sqs_client, name="ingests"
    )

    print(f"ingests topic arn = {underlined(ingests_topic_arn)}")
    print(f"ingests queue url = {underlined(ingests_queue_url)}")

    sqs_messages = sqs_client.receive_message(QueueUrl=bag_unpacker_queue_url)
    print(sqs_messages)
#
    ingests_table_resp = dynamodb_client.create_table(
        TableName="ingests",
        KeySchema=[{"AttributeName": "id", "KeyType": "HASH"}],
        AttributeDefinitions=[{"AttributeName": "id", "AttributeType": "S"}],
        ProvisionedThroughput={"ReadCapacityUnits": 1, "WriteCapacityUnits": 1},
    )
    print(f"ingests table name = {underlined('ingests')}")
    # print(dynamodb_client.scan(TableName="ingests"))

