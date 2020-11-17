import boto3
import moto

from helpers import read_secret, write_secret


@moto.mock_secretsmanager
def test_can_store_secret():
    """
    We can store a Secret and retrieve it.
    """
    client = boto3.client("secretsmanager")

    write_secret(client, id="MyPassword", value="correct-horse-battery-staple")
    assert read_secret(client, id="MyPassword") == "correct-horse-battery-staple"


@moto.mock_secretsmanager
def test_can_overwrite_secret():
    """
    We can store a Secret, then overwrite it with a new value.
    """
    client = boto3.client("secretsmanager")

    write_secret(client, id="MyPassword", value="correct-horse-battery-staple")
    assert read_secret(client, id="MyPassword") == "correct-horse-battery-staple"

    write_secret(client, id="MyPassword", value="Tr0ub4dor&3")
    assert read_secret(client, id="MyPassword") == "Tr0ub4dor&3"
