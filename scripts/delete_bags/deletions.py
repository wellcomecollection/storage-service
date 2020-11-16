import datetime

from iam import (
    DEV_ROLE_ARN,
    create_dynamo_client_from_role_arn,
    get_underlying_role_arn,
)


def record_deletion(*, ingest_id, space, external_identifier, version, reason):
    event = {
        "requested_by": get_underlying_role_arn(),
        "deleted_at": datetime.datetime.now().isoformat(),
        "reason": reason,
    }

    dynamodb = create_dynamo_client_from_role_arn(role_arn=DEV_ROLE_ARN)
    dynamodb.update_item(
        TableName="deleted_bags",
        Key={"ingest_id": ingest_id},
        UpdateExpression="""
            SET
            #space = :space,
            #externalIdentifier = :externalIdentifier,
            #version = :version,
            #events = list_append(if_not_exists(#events, :empty_list), :event)
        """,
        ExpressionAttributeNames={
            "#space": "space",
            "#externalIdentifier": "externalIdentifier",
            "#version": "version",
            "#events": "events",
        },
        ExpressionAttributeValues={
            ":space": space,
            ":externalIdentifier": external_identifier,
            ":version": version,
            ":event": [event],
            ":empty_list": [],
        },
    )
