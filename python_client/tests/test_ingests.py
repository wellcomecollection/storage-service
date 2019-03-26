# -*- encoding: utf-8

import pytest

from wellcome_storage_service.exceptions import IngestNotFound, ServerError, UserError


def test_can_create_client(client):
    resp = client.get_ingest(ingest_id="017e60b8-9779-463f-821e-321a20f7edc4")

    assert isinstance(resp, dict)
    assert resp["id"] == "017e60b8-9779-463f-821e-321a20f7edc4"
    assert resp["status"]["id"] == "succeeded"


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


def test_can_create_ingest(client):
    location = client.create_s3_ingest(
        space_id="digitised", s3_bucket="testing-bucket", s3_key="bagit.zip"
    )

    assert location.endswith("/ingests/fb1e3805-810d-4822-8eeb-e1d45a357a6b")

    resp = client.get_ingest_from_location(location)
    assert resp["id"] == "fb1e3805-810d-4822-8eeb-e1d45a357a6b"
    assert resp["sourceLocation"]["provider"]["id"] == "aws-s3-standard"
    assert resp["sourceLocation"]["bucket"] == "testing-bucket"
    assert resp["sourceLocation"]["path"] == "bagit.zip"
    assert "callback" not in resp


def test_can_create_ingest_with_callback(client):
    location = client.create_s3_ingest(
        space_id="digitised",
        s3_bucket="testing-bucket",
        s3_key="bagit.zip",
        callback_url="https://example.org/callback/bagit.zip",
    )

    resp = client.get_ingest_from_location(location)
    assert resp["callback"]["url"] == "https://example.org/callback/bagit.zip"
