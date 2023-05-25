from elasticsearch import Elasticsearch

from .secrets import read_secret


def get_reporting_client(secrets_client, *, environment, app_name):
    """
    Gets an Elasticsearch client authenticated against the reporting cluster
    with a given app's set of credentials.
    """
    username = read_secret(
        secrets_client, id=f"{environment}/indexer/{app_name}/es_username"
    )
    password = read_secret(
        secrets_client, id=f"{environment}/indexer/{app_name}/es_password"
    )
    port = read_secret(secrets_client, id=f"{environment}/indexer/es_port")
    host = read_secret(secrets_client, id=f"{environment}/indexer/es_host")

    endpoint = f"https://{host}:{port}"

    return Elasticsearch(
        endpoint, http_auth=(username, password)
    )
