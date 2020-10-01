import boto3


secrets_client = boto3.client("secretsmanager")


def get_secret(secret_id):
    """
    Get a Secret from Secrets Manager.
    """
    return secrets_client.get_secret_value(SecretId=secret_id)["SecretString"]
