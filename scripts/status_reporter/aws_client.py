import uuid

import boto3


class AwsClient:
    def __init__(self, role_arn):
        self.role_arn = role_arn

    def _assumed_role_session(self):
        session = boto3.session.Session()
        sts_client = session.client("sts")

        assumed_role_object = sts_client.assume_role(
            RoleArn=self.role_arn, RoleSessionName=str(uuid.uuid1().hex)
        )

        credentials = assumed_role_object["Credentials"]

        session = boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

        return session

    def s3_client(self):
        session = self._assumed_role_session()
        return session.client("s3")


read_only_client = AwsClient(
    role_arn="arn:aws:iam::975596993436:role/storage-read_only"
)
