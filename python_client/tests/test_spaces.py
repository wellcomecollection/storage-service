# -*- encoding: utf-8

import pytest


def test_get_spaces_is_notimplemented(client):
    with pytest.raises(NotImplementedError):
        client.get_space(space_id="digitised")
