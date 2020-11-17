from botocore.exceptions import ClientError


def write_secret(secrets_client, *, id, value):
    """
    Store a new secret in Secrets Manager, or update an existing secret.
    """
    try:
        secrets_client.put_secret_value(SecretId=id, SecretString=value)
    except ClientError as err:  # pragma: no cover
        if err.response["Error"]["Code"] == "ResourceNotFoundException":
            secrets_client.create_secret(Name=id, SecretString=value)
        else:
            raise


def read_secret(secrets_client, *, id):
    """
    Retrieve a secret from Secrets Manager.
    """
    return secrets_client.get_secret_value(SecretId=id)["SecretString"]
