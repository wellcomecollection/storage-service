import contextlib
import json
import subprocess


def az(*args, **kwargs):
    """
    Run a command with the Azure CLI.
    """
    cmd = ["az"] + list(args) + ["--output=json"]
    output = subprocess.check_output(cmd, **kwargs)
    return json.loads(output)


def _get_legal_hold_tags(*, account, container):
    """
    Returns the list of legal hold tags on a container, if any.
    """
    # This command returns output of the form:
    #
    #       {
    #         "hasLegalHold": true,
    #         "tags": [
    #           {
    #             "tag": "storageservice",
    #             ...
    #           }
    #         ]
    #       }
    #
    # See https://docs.microsoft.com/en-us/cli/azure/storage/container/legal-hold?view=azure-cli-latest#az_storage_container_legal_hold_show
    existing_hold = az(
        "storage",
        "container",
        "legal-hold",
        "show",
        "--account-name",
        account,
        "--container-name",
        container,
    )

    if not existing_hold["hasLegalHold"]:
        assert existing_hold["tags"] == [], existing_hold
        return []
    else:
        assert len(existing_hold["tags"]) > 0, existing_hold
        return [t["tag"] for t in existing_hold["tags"]]


@contextlib.contextmanager
def unlocked_azure_container(*, account, container):
    """
    By default, we have legal holds on our Azure containers that prevent us from
    deleting any blobs.

    These legal holds are not "locked" -- they can be disabled or enabled at any
    time, and they apply to the entire container.  This context manager temporarily
    unlocks a container, allowing us to delete blobs.

    Use it like so:

        with unlocked_container(account, container):
            # do stuff in container

    The container will be automatically locked when the caller leaves the 'with' block.

    """
    # First work out what legal holds we have on a container, if any.  We'll
    # remember these tags and re-apply them when we've finished modifying the
    # container.
    existing_legal_hold_tags = _get_legal_hold_tags(
        account=account, container=container
    )

    # If there's no legal hold, this function is a no-op -- yield control so the
    # caller can run the contents of the 'with' block, then give up.
    if not existing_legal_hold_tags:
        yield
        return

    # Now remove all the existing legal hold tags.  We wrap all of this in a
    # try … finally block, so if anything goes wrong the legal hold should
    # be restored.
    try:

        # It's possible to remove multiple legal hold tags in one command,
        # but it's not clear from the docs.  You need to supply the tags as
        # separate arguments, e.g. `--tags tag1 tag2`
        #
        # See https://github.com/Azure/azure-cli/issues/15965
        # See https://github.com/Azure/azure-cli/blob/a2f2ec07c93e46103d703f98710e5756acfab085/src/azure-cli/azure/cli/command_modules/storage/tests/latest/test_storage_container_legal_hold.py#L29-L31
        # See https://docs.microsoft.com/en-us/cli/azure/storage/container/legal-hold?view=azure-cli-latest#az_storage_container_legal_hold_clear
        az(
            "storage",
            "container",
            "legal-hold",
            "clear",
            "--account-name",
            account,
            "--container-name",
            container,
            "--tags",
            *existing_legal_hold_tags,
        )

        # Check we really have removed all the legal hold tags, or the inner
        # body of the 'with' block will fail.
        assert (
            _get_legal_hold_tags(account=account, container=container) == []
        ), f"Unable to remove legal hold tags from Azure container {container}"

        yield
    finally:

        # Similarly to deleting, it's possible to set multiple legal holds in
        # one command, using the same syntax, just not documented.
        #
        # See https://docs.microsoft.com/en-us/cli/azure/storage/container/legal-hold?view=azure-cli-latest#az_storage_container_legal_hold_set
        az(
            "storage",
            "container",
            "legal-hold",
            "set",
            "--account-name",
            account,
            "--container-name",
            container,
            "--tags",
            *existing_legal_hold_tags,
        )

        # Check we really did restore all the previous legal hold tags, or
        # the container is in a dangerous state.
        assert (
            _get_legal_hold_tags(account=account, container=container)
            == existing_legal_hold_tags
        ), (
            f"Unable to restore legal hold tags from Azure container {container} "
            f" ({' '.join(existing_legal_hold_tags)})"
        )
