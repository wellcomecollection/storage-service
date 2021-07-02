#!/usr/bin/env python

import base64

import boto3


def get_aws_session(*, role_arn, **kwargs):
    sts_client = boto3.client("sts")
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]
    return boto3.Session(
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
        **kwargs
    )


if __name__ == '__main__':
    session = get_aws_session(
        role_arn="arn:aws:iam::975596993436:role/storage-ci",
        region_name="us-east-1"
    )

    client = session.client("ecr-public")
    token_resp = client.get_authorization_token()

    auth_data = base64.b64decode(token_resp["authorizationData"]["authorizationToken"])
    username, password = auth_data.split(b":")

    print(password.decode("utf8"))
