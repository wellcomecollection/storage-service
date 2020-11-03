import pytest

from miro_shards import choose_miro_shard


@pytest.mark.parametrize("miro_id, expected_miro_shard", [("A0000001", "A0000000")])
def test_choose_miro_shard(miro_id, expected_miro_shard):
    assert choose_miro_shard(miro_id) == expected_miro_shard
