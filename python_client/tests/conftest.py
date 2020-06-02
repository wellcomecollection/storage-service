# -*- encoding: utf-8

import json
import os

import betamax
from betamax.cassette import cassette
from betamax_serializers.pretty_json import PrettyJSONSerializer
import pytest

from wellcome_storage_service import RequestsOAuthStorageServiceClient


# Remove our OAuth authorization token from betamax recordings.  This is
# based on an example from the Betamax docs:
# https://betamax.readthedocs.io/en/latest/configuring.html#filtering-sensitive-data
def sanitize_token(interaction, current_cassette):
    req_headers = interaction.data["request"]["headers"]
    token = req_headers["Authorization"]

    current_cassette.placeholders.append(
        cassette.Placeholder(placeholder="<AUTH_TOKEN>", replace=token[0])
    )

    resp_body = interaction.data["response"]["body"]["string"]
    try:
        access_token = json.loads(resp_body)["access_token"]

        # Create different placeholders ACCESS_TOKEN_1, ACCESS_TOKEN_2, and so on,
        # so we can check when the storage service has sent different tokens.
        placeholder = "<ACCESS_TOKEN_%d>" % len(current_cassette.placeholders)

        current_cassette.placeholders.append(
            cassette.Placeholder(placeholder=placeholder, replace=access_token)
        )
    except KeyError:
        pass


betamax.Betamax.register_serializer(PrettyJSONSerializer)

with betamax.Betamax.configure() as config:
    config.cassette_library_dir = "tests/cassettes"
    config.before_record(callback=sanitize_token)
    config.default_cassette_options["serialize_with"] = PrettyJSONSerializer.name


@pytest.fixture
def client(request):
    client_id = os.environ.get("CLIENT_ID", "test_client_id")
    client_secret = os.environ.get("CLIENT_SECRET", "test_client_secret")
    token_url = "https://auth.wellcomecollection.org/oauth2/token"

    config.define_cassette_placeholder("<CLIENT_ID>", client_id)
    config.define_cassette_placeholder("<CLIENT_SECRET>", client_secret)

    ss_client = RequestsOAuthStorageServiceClient(
        api_url="https://api.wellcomecollection.org/storage/v1",
        client_id=client_id,
        client_secret=client_secret,
        token_url=token_url,
    )

    # Store an individual cassette for each test.
    # See https://stackoverflow.com/q/17726954/1558022
    with betamax.Betamax(ss_client.sess).use_cassette(request.node.name):
        yield ss_client
