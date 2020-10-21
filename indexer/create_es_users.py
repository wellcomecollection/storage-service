#!/usr/bin/env python
"""
This script creates roles and users that allow the indexer services to talk to
the reporting cluster.

You can use your own credentials, or generate a new password for
the 'elastic' user by logging in to the Elastic Cloud console.
"""

import functools
import secrets

import boto3
from botocore.exceptions import ClientError
import click
from elasticsearch import Elasticsearch
import hyperlink


DEFAULT_DESCRIPTION = "Credentials for the reporting cluster"


@functools.lru_cache()
def get_aws_client(resource, *, role_arn):
    """
    Get a boto3 client authenticated against the given role.
    """
    sts_client = boto3.client("sts")
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]
    return boto3.client(
        resource,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )


def store_secret(secret_id, secret_value, description=DEFAULT_DESCRIPTION):
    """
    Store a key/value pair in Secrets Manager.
    """
    role_arn = "arn:aws:iam::975596993436:role/storage-developer"
    secrets_client = get_aws_client("secretsmanager", role_arn=role_arn)

    try:
        resp = secrets_client.create_secret(
            Name=secret_id, Description=description, SecretString=secret_value
        )
    except ClientError as err:
        if err.response["Error"]["Code"] == "ResourceExistsException":
            resp = secrets_client.put_secret_value(
                SecretId=secret_id, SecretString=secret_value
            )

            if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
                raise RuntimeError(f"Unexpected error from PutSecretValue: {resp}")
        else:
            raise
    else:
        if resp["ResponseMetadata"]["HTTPStatusCode"] != 200:
            raise RuntimeError(f"Unexpected error from CreateSecret: {resp}")

    click.echo(f"Stored secret {click.style(secret_id, 'yellow')}")


def create_roles(es, *, index_prefix, doc_type):
    """
    Create read and write roles for a given work type.
    """
    for role_suffix, privileges in [("read", ["read"]), ("write", ["all"])]:
        role_name = f"{index_prefix}_{doc_type}_{role_suffix}"
        index_pattern = f"{index_prefix}_{doc_type}*"

        es.security.put_role(
            role_name,
            body={"indices": [{"names": [index_pattern], "privileges": privileges}]},
        )

        username = role_name

        password = secrets.token_hex()
        es.security.put_user(username=username, body={"password": password, "roles": [role_name]})

        yield (role_name, username, password)


@click.command()
@click.option(
    "--username", default="elastic", prompt="What is your Elasticsearch username?"
)
@click.option(
    "--password", hide_input=True, prompt="What is your Elasticsearch password?"
)
@click.option("--endpoint", prompt="What is your Elasticsearch endpoint?")
def main(username, password, endpoint):
    url = hyperlink.URL.from_text(endpoint)
    host = url.host
    protocol = url.scheme
    port = str(url.port)

    click.echo(
        f"Detected the host as {click.style(url.host, 'blue')}, the port as "
        f"{click.style(port, 'blue')} and the protocol as {click.style(protocol, 'blue')}."
    )
    click.confirm("Are these correct?", abort=True)

    es = Elasticsearch(endpoint, http_auth=(username, password))

    print("")

    for env, index_prefix in [
        ("staging", "storage_stage"),
        ("prod", "storage")
    ]:
        store_secret(secret_id=f"{env}/indexer/es_host", secret_value=host)
        store_secret(secret_id=f"{env}/indexer/es_port", secret_value=port)
        store_secret(secret_id=f"{env}/indexer/es_protocol", secret_value=protocol)

        print("")

        for doc_type in ("bags", "files", "ingests"):
            for role_name, username, password in create_roles(es, index_prefix=index_prefix, doc_type=doc_type):
                click.echo(f"Created role {click.style(role_name, 'green')}")
                click.echo(f"Created user {click.style(username, 'green')}")

                if username.endswith("_write"):
                    store_secret(secret_id=f"{env}/indexer/{doc_type}/es_username", secret_value=username)
                    store_secret(secret_id=f"{env}/indexer/{doc_type}/es_password", secret_value=password)

            print("")


if __name__ == "__main__":
    main()
