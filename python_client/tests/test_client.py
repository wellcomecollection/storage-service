import json
import os
import random
import string

import pytest

from wellcome_storage_service import (
    DEFAULT_CREDENTIALS_PATH,
    RequestsOAuthStorageServiceClient
)


def rand_hex():
    return "".join(random.choice(string.hexdigits) for _ in range(4))


def test_can_get_requests_oauth_client_from_path(tmpdir):
    credentials_path = tmpdir / "oauth_credentials.json"

    client_id = "client_id-%s" % rand_hex()
    client_secret = "client_secret-%s" % rand_hex()
    token_url = "https://example.org/api/v1/token"

    credentials_path.write(json.dumps({
        "client_id": client_id,
        "client_secret": client_secret,
        "token_url": token_url,
    }))

    client = RequestsOAuthStorageServiceClient.from_path(
        api_url="https://example.org/api/v1/storage",
        credentials_path=str(credentials_path)
    )

    assert client.api_url == "https://example.org/api/v1/storage"
    assert client.client_id == client_id
    assert client.client_secret == client_secret
    assert client.token_url == token_url


@pytest.mark.skipif(
    not os.path.exists(DEFAULT_CREDENTIALS_PATH),
    reason="Default credentials are not available"
)
def test_gets_credentials_from_default_path(tmpdir):
    """
    If a path isn't supplied, it can still find credentials at the default path.
    """
    client = RequestsOAuthStorageServiceClient.from_path(
        api_url="https://example.org/api/v1/storage"
    )

    assert client.client_id == json.load(open(DEFAULT_CREDENTIALS_PATH))["client_id"]
