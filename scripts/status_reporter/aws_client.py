import uuid

import boto3


class AwsClient:
    def __init__(self, role_arn, assume_role=False):
        self.role_arn = role_arn
        self._aws = boto3

        if assume_role:
            self._aws = self._assume_role_session()

    def _assume_role_session(self):
        session = boto3.session.Session()
        sts_client = session.client("sts")

        assumed_role_object = sts_client.assume_role(
            RoleArn=self.role_arn, RoleSessionName=str(uuid.uuid1().hex)
        )

        credentials = assumed_role_object["Credentials"]

        return boto3.Session(
            aws_access_key_id=credentials["AccessKeyId"],
            aws_secret_access_key=credentials["SecretAccessKey"],
            aws_session_token=credentials["SessionToken"],
        )

    def s3_client(self):
        return self._aws.client("s3")

    def dynamo_client(self):
        return self._aws.client("dynamodb")

    def dynamo_resource(self):
        return self._aws.resource("dynamodb")

    def dynamo_table(self, table_name):
        return self.dynamo_resource().Table(table_name)

    def secrets_manager_value(self, secret_name):
        client = self._aws.client("secretsmanager")
        return client.get_secret_value(SecretId=secret_name)["SecretString"]


read_only_client = AwsClient(
    role_arn="arn:aws:iam::975596993436:role/storage-read_only"
)

dev_client = AwsClient(role_arn="arn:aws:iam::975596993436:role/storage-developer")
