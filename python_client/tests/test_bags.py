# -*- encoding: utf-8

import pytest


def test_can_get_bag(client):
    resp = client.get_bag(space_id="digitised", source_id="b12332142")
    from pprint import pprint
    pprint(resp)
    assert 0
