"""
Regenerate the account keys for our Azure storage account.

Use this script if you suspect the keys have been leaked or compromised.

It will automatically regenerate new credentials for the replicator unless you
tell it not to by passing --skip-replicator-credentials as a flag.
"""

import subprocess
import sys

import termcolor

from _azure import az, get_storage_accounts


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
        resource_group = account["resourceGroup"]
        log_event(f"Renewing account keys for {account_name}...")

        for key in ("primary", "secondary"):
            az(
                "storage",
                "account",
                "keys",
                "renew",
                "--key",
                key,
                "--account-name",
                account_name,
                "--resource-group",
                resource_group,
            )

        log_outcome(f"Renewed primary and secondary account keys for {account_name}")

    print("")

    if "--skip-replicator-credentials" in sys.argv:
        log_event("Not creating new replicator credentials")
    else:
        log_event("Creating new replicator credentials")
        print("")
        subprocess.check_call(
            [sys.executable, "azure_create_replicator_credentials.py", "--skip-login"]
        )
