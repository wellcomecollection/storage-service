import boto3
import httpx


secrets_client = boto3.client("secretsmanager")


def get_secret(secret_id):
    """
    Get a Secret from Secrets Manager.
    """
    return secrets_client.get_secret_value(SecretId=secret_id)["SecretString"]


def get_es_client():
    """
    Get an instance of httpx.Client authenticated to the reporting cluster.
    """
    es_host = get_secret("storage_service_reporter/es_host")
    es_port = get_secret("storage_service_reporter/es_port")
    es_user = get_secret("storage_service_reporter/es_user")
    es_pass = get_secret("storage_service_reporter/es_pass")

    return httpx.Client(
        base_url=f"https://{es_host}:{es_port}",
        auth=(es_user, es_pass)
    )
