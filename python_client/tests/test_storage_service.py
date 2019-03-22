#!/usr/bin/env python
# -*- encoding: utf-8

import pytest

from wellcome_storage_service.exceptions import IngestNotFound


def test_can_create_client(client):
    resp = client.get_ingest(ingest_id="017e60b8-9779-463f-821e-321a20f7edc4")

    assert isinstance(resp, dict)
    assert resp["id"] == "017e60b8-9779-463f-821e-321a20f7edc4"
    assert resp["status"]["id"] == "succeeded"


def test_get_missing_ingest_is_error(client):
    with pytest.raises(IngestNotFound):
        client.get_ingest(ingest_id="1e4b384e-857b-49aa-a1cb-6c57762b0c3f")
