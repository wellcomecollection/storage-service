import json
import subprocess

from azure.storage.blob import (
    generate_container_sas,
    BlobSasPermissions,
    BlobServiceClient,
)

from _aws import get_elastic_ip


def az(*args):
    return subprocess.check_output(["az"] + list(args)).decode("utf8").strip()


def _create_sas_uris(connection_string, *, expiry, ip):
    """
    Creates read/write SAS URIs for all the containers in our Azure storage account.

    Generates (container_name, sas_uri) pairs.
    """
    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    for container in blob_service_client.list_containers():
        container_name = container["name"]

        token = generate_container_sas(
            blob_service_client.account_name,
            container_name=container_name,
            account_key=blob_service_client.credential.account_key,
            expiry=expiry,
            ip=ip,
            # These permissions are fairly blunt -- there's a single "write"
            # permission for any modifications to a blob, whether that's the
            # content or the metadata.
            permission=BlobSasPermissions(read=True, write=True, delete=False),
        )

        yield (container_name, f"{blob_service_client.url}?{token}")


def create_prod_sas_uris(connection_string):
    """
    Creates read/write SAS URIs for services running in our AWS account.

    These URIs are:
    -   set to never expire
    -   bound to the Elastic IP of the storage account

    We consider IP restrictions a good enough security check on their use.

    """
    elastic_ip = get_elastic_ip()

    yield from _create_sas_uris(
        connection_string=connection_string, expiry=None, ip=elastic_ip
    )


def get_storage_accounts():
    """
    Finds all the storage accounts a user has access to.
    """
    return json.loads(az("storage", "account", "list"))


def get_connection_string(account_name):
    """
    Gets the connection string for an Azure account.
    """
    return json.loads(
        az("storage", "account", "show-connection-string", "--name", account_name)
    )["connectionString"]
