# -*- encoding: utf-8

import pytest

from wellcome_storage_service.exceptions import BagNotFound


def test_can_get_bag(client):
    resp = client.get_bag(space_id="alex-testing", source_id="b12345")
    assert resp["space"]["id"] == "alex-testing"
    assert resp["id"] == "alex-testing/b12345"
    assert len(resp["manifest"]["files"]) == 3


def test_can_get_bag_version(client):
    resp = client.get_bag(space_id="alex-testing", source_id="b12345", version="v1")
    assert resp["space"]["id"] == "alex-testing"
    assert resp["id"] == "alex-testing/b12345"
    assert resp["version"] == "v1"
    assert len(resp["manifest"]["files"]) == 3


def test_gets_correct_bag_version(client):
    resp = client.get_bag(space_id="alex-testing", source_id="b12345", version="v3")
    assert resp["version"] == "v3"


def test_raises_404_for_missing_bag(client):
    expected_message = "Bags API returned 404 for bag alex-testing/doesnotexist"
    with pytest.raises(BagNotFound, match=expected_message):
        client.get_bag(space_id="alex-testing", source_id="doesnotexist")


def test_raises_404_for_missing_bag_with_version(client):
    expected_message = (
        "Bags API returned 404 for bag alex-testing/doesnotexist with version v1"
    )
    with pytest.raises(BagNotFound, match=expected_message):
        client.get_bag(space_id="alex-testing", source_id="doesnotexist", version="v1")
