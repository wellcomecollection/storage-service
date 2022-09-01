import pytest

from wellcome_storage_service.exceptions import IngestNotFound, ServerError, UserError


def test_get_missing_ingest_is_error(client):
    with pytest.raises(IngestNotFound):
        client.get_ingest(ingest_id="1e4b384e-857b-49aa-a1cb-6c57762b0c3f")


def test_4xx_response_becomes_usererror(client):
    with pytest.raises(UserError):
        client.get_ingest(ingest_id="doesnotexist")


def test_5xx_response_becomes_servererror(client):
    # I don't know how to trigger reliable 500s in the API, so this test is
    # using a hand-written Betamax cassette.
    with pytest.raises(ServerError):
        client.get_ingest(ingest_id="trigger_server_error")


def test_can_create_and_retrieve_ingest(client):
    location = client.create_s3_ingest(
        space="digitised",
        external_identifier="b12345",
        s3_bucket="testing-bucket",
        s3_key="bagit.zip",
    )

    resp = client.get_ingest_from_location(location)
    ingest_id = resp["id"]
    assert location.endswith("/ingests/%s" % ingest_id)
    assert resp["sourceLocation"]["provider"]["id"] == "aws-s3-standard"
    assert resp["sourceLocation"]["bucket"] == "testing-bucket"
    assert resp["sourceLocation"]["path"] == "bagit.zip"
    assert "callback" not in resp


def test_can_create_ingest_with_callback(client):
    location = client.create_s3_ingest(
        space="digitised",
        external_identifier="b12345",
        s3_bucket="testing-bucket",
        s3_key="bagit.zip",
        callback_url="https://example.org/callback/bagit.zip",
    )

    resp = client.get_ingest_from_location(location)
    assert resp["callback"]["url"] == "https://example.org/callback/bagit.zip"


def test_default_ingest_type_is_create(client):
    location = client.create_s3_ingest(
        space="digitised",
        external_identifier="b12345",
        s3_bucket="testing-bucket",
        s3_key="bagit.zip",
    )

    resp = client.get_ingest_from_location(location)
    assert resp["ingestType"]["id"] == "create"


@pytest.mark.parametrize("ingest_type", ["create", "update"])
def test_can_specify_ingest_type(client, ingest_type):
    location = client.create_s3_ingest(
        space="digitised",
        external_identifier="b12345",
        s3_bucket="testing-bucket",
        s3_key="bagit.zip",
        ingest_type=ingest_type,
    )

    resp = client.get_ingest_from_location(location)
    assert resp["ingestType"]["id"] == ingest_type


def test_it_includes_the_external_identifier(client):
    external_identifier = "b12345"
    location = client.create_s3_ingest(
        space="digitised",
        external_identifier=external_identifier,
        s3_bucket="testing-bucket",
        s3_key="bagit.zip",
    )

    resp = client.get_ingest_from_location(location)
    assert resp["bag"]["info"]["externalIdentifier"] == external_identifier


def test_it_wraps_a_4xx_as_a_usererror(client):
    with pytest.raises(
        UserError,
        match="Bad Request: Invalid value at .space.id: must not contain slashes.",
    ):
        client.create_s3_ingest(
            space="space/with/a/slash",
            external_identifier="b12345",
            s3_bucket="testing-bucket",
            s3_key="bagit.zip",
        )
