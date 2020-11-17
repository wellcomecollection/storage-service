import boto3
import moto
import pytest

from helpers import read_secret, write_secret


@pytest.fixture
def client():
    with moto.mock_secretsmanager():
        yield boto3.client("secretsmanager", region_name="eu-west-1")


def test_can_store_secret(client):
    """
    We can store a Secret and retrieve it.
    """
    write_secret(client, id="MyPassword", value="correct-horse-battery-staple")
    assert read_secret(client, id="MyPassword") == "correct-horse-battery-staple"


def test_can_overwrite_secret(client):
    """
    We can store a Secret, then overwrite it with a new value.
    """
    write_secret(client, id="MyPassword", value="correct-horse-battery-staple")
    assert read_secret(client, id="MyPassword") == "correct-horse-battery-staple"

    write_secret(client, id="MyPassword", value="Tr0ub4dor&3")
    assert read_secret(client, id="MyPassword") == "Tr0ub4dor&3"
