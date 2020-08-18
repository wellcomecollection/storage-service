#!/usr/bin/env python
"""
Create an SAS Container URI for our Azure Blob Storage account.

This allows somebody using this URI as their Connection String to read/write
to our Azure Blob Storage containers.

The script creates one URI per container, and writes them to Secrets Manager.

The URIs will auto-expire after a certain period, or you can revoke them immediately
by rotating the storage Access Key in the Azure console.

"""

import sys

import termcolor

from _aws import store_secret
from _azure import az, create_prod_sas_uris, get_connection_string, get_storage_accounts


def log_event(s):
    print(termcolor.colored(s, "blue"))


def log_outcome(s):
    print(termcolor.colored(s, "yellow"))


if __name__ == "__main__":
    if "--skip-login" not in sys.argv:
        log_event("Logging in to Azure...")
        az("login")

        print("")

    log_event("Looking up your storage accounts...")
    storage_accounts = get_storage_accounts()
    log_outcome(
        f"You have access to {len(storage_accounts)} storage account{'s' if len(storage_accounts) != 1 else ''}"
    )

    for account in storage_accounts:
        print("")
        account_name = account["name"]
        log_event(f"Getting connection string for {account_name}...")
        connection_string = get_connection_string(account_name)

        log_event(f"Creating SAS URIs for containers in {account_name}...")

        for container_name, mode, sas_uri in create_prod_sas_uris(connection_string):
            secret_id = f"azure/{account_name}/{container_name}/{mode}_sas_url"
            store_secret(secret_id=secret_id, secret_string=sas_uri)
            log_outcome(
                f"Stored secret:\n - container: {container_name}\n - secret ID: {secret_id}"
            )
