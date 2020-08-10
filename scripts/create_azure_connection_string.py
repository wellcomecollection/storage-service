#!/usr/bin/env python
"""
Create an SAS Container URI for our Azure Blob Storage account.

This allows somebody using this URI as their Connection String to read/write
to our Azure Blob Storage containers.

The script creates one URI per container, and writes them to Secrets Manager.

The URIs will auto-expire after a certain period, or you can revoke them immediately
by rotating the storage Access Key in the Azure console.

"""

import datetime
import getpass

from azure.storage.blob import (
    generate_container_sas,
    BlobSasPermissions,
    BlobServiceClient,
)
import termcolor

from aws import store_secret
from common import get_aws_client


if __name__ == "__main__":
    connection_string = getpass.getpass(
        "What is the connection string for the storage account? "
    )

    blob_service_client = BlobServiceClient.from_connection_string(connection_string)

    secrets_client = get_aws_client(
        "secretsmanager", role_arn="arn:aws:iam::975596993436:role/storage-developer"
    )

    for container in blob_service_client.list_containers():
        container_name = container["name"]

        token = generate_container_sas(
            blob_service_client.account_name,
            container_name=container_name,
            account_key=blob_service_client.credential.account_key,
            # These permissions are fairly blunt -- there's a single "write"
            # permission for any modifications to a blob, whether that's the
            # content or the metadata.
            permission=BlobSasPermissions(read=True, write=True, delete=False),
            # TODO: Decide how long we actually want these credentials to last.
            expiry=datetime.datetime.utcnow() + datetime.timedelta(days=1),
            # TODO: Passing the `ip` parameter allows you to restrict the IP range
            # that this SAS URI can be used with.  We should do this when we
            # start to run in production.
        )

        new_url = f"{blob_service_client.url}?{token}"

        secret_id = f"azure/{blob_service_client.account_name}/{container_name}/read_write_sas_url"

        print(
            "Writing SAS URI for container %s to %s"
            % (
                termcolor.colored(container_name, "blue"),
                termcolor.colored(secret_id, "green"),
            )
        )

        store_secret(secrets_client, secret_id=secret_id, secret_string=new_url)
