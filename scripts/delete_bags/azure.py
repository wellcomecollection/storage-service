import contextlib
import json
import subprocess


ACCOUNT_NAME_LOOKUP = {
    "wellcomecollection-storage-staging-replica-netherlands": "wecostoragestage",
    "wellcomecollection-storage-replica-netherlands": "wecostorageprod",
}


def az(*args, suppress_stderr=False):
    result = subprocess.check_output(
        ["az"] + list(args),
        stderr=subprocess.DEVNULL if suppress_stderr else subprocess.PIPE,
    )
    return result.decode("utf8").strip()


@contextlib.contextmanager
def unlocked_container(container_name):
    """
    By default, we have legal holds on our Azure containers that prevent us from
    deleting any blobs.

    These legal holds are not "locked" -- they can be disabled or enabled at any
    time, and they apply to the entire container.  This context manager temporarily
    unlocks a container, allowing us to delete blobs.

    Use it like so:

        with unlocked_container(container_name):
            # do stuff in container

    The container will be automatically locked when the caller leaves the 'with' block.

    """
    account_name = ACCOUNT_NAME_LOOKUP[container_name]

    resp = az(
        "storage",
        "container",
        "legal-hold",
        "clear",
        f"--account-name={account_name}",
        f"--container-name={container_name}",
        "--tags=storageservice",
    )

    resp = json.loads(resp)

    if resp["hasLegalHold"]:
        raise RuntimeError(
            "Unable to clear legal hold on Azure container {container_name} ({resp})"
        )

    try:
        yield
    finally:
        az(
            "storage",
            "container",
            "legal-hold",
            "set",
            f"--account-name={account_name}",
            f"--container-name={container_name}",
            "--tags=storageservice",
        )


def list_blobs_under(*, container_name, prefix):
    """
    Lists all the blobs under a given Azure prefix.
    """
    account_name = ACCOUNT_NAME_LOOKUP[container_name]

    resp = az(
        "storage",
        "blob",
        "list",
        f"--account-name={account_name}",
        f"--container-name={container_name}",
        f"--prefix={prefix}",
        # Return all the results!
        "--num-results=*",
        # Otherwise we get lots of warnigns:
        # Please provide --connection-string, --account-key or --sas-token as credential, or use `--auth-mode login` if you have required RBAC roles in your command. For more information about RBAC roles in storage, you can see https://docs.microsoft.com/en-us/azure/storage/common/storage-auth-aad-rbac-cli.
        suppress_stderr=True,
    )

    for azure_obj in json.loads(resp):
        yield azure_obj["name"]


def delete_blob(*, container_name, blob_name):
    """
    Deletes an individual Azure blob.
    """
    account_name = ACCOUNT_NAME_LOOKUP[container_name]

    az(
        "storage",
        "blob",
        "delete",
        f"--account-name={account_name}",
        f"--container-name={container_name}",
        f"--name={blob_name}",
        suppress_stderr=True,
    )
