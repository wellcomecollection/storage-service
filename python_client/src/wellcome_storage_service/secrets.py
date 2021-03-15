"""
Fetches credentials for the dev client from Secrets Manager.
"""

import functools

import boto3


def get_session():
    """
    Creates a boto3 Session with the storage-developer role.
    """
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"
    sts_client = boto3.client("sts")
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]
    return boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )


def get_secrets():
    session = get_session()
    secrets_client = session.client("secretsmanager")

    client_id = secrets_client.get_secret_value(SecretId="dev_testing/client_id")["SecretString"]
    client_secret = secrets_client.get_secret_value(SecretId="dev_testing/client_secret")["SecretString"]

    return {"client_id": client_id, "client_secret": client_secret}
