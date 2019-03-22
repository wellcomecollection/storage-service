#!/usr/bin/env python
# -*- encoding: utf-8


def test_can_create_client(client):
    resp = client.get_ingest(ingest_id="017e60b8-9779-463f-821e-321a20f7edc4")

    assert isinstance(resp, dict)
    assert resp["id"] == "017e60b8-9779-463f-821e-321a20f7edc4"
    assert resp["status"]["id"] == "succeeded"
