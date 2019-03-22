#!/usr/bin/env python
# -*- encoding: utf-8

import betamax

from wellcome_storage_service import StorageServiceClient


from betamax.cassette import cassette


def sanitize_token(interaction, current_cassette):
    # Exit early if the request did not return 200 OK because that's the
    # only time we want to look for Authorization-Token headers
    if interaction.data['response']['status']['code'] != 200:
        return

    headers = interaction.data['request']['headers']
    token = headers.get('Authorization')
    # If there was no token header in the response, exit
    if token is None:
        return

    # Otherwise, create a new placeholder so that when cassette is saved,
    # Betamax will replace the token with our placeholder.
    current_cassette.placeholders.append(
        cassette.Placeholder(placeholder='<AUTH_TOKEN>', replace=token)
    )


with betamax.Betamax.configure() as config:
    config.cassette_library_dir = 'tests/cassettes'
    config.before_record(callback=sanitize_token)


def test_can_create_client():
    client = StorageServiceClient(api_url="http://example.org", sess=None)

    import json
    client = StorageServiceClient.with_oauth(
        api_url="https://api.wellcomecollection.org/storage/v1",
        **json.load(open("/Users/chana/repos/storage-service/bin/oauth-credentials.json"))
    )



    with betamax.Betamax(client.sess).use_cassette('repo'):
        resp = client.get_ingest(ingest_id="017e60b8-9779-463f-821e-321a20f7edc4")

        assert isinstance(resp, dict)
        assert resp["id"] == "017e60b8-9779-463f-821e-321a20f7edc4"
        assert resp["status"]["id"] == "succeeded"
