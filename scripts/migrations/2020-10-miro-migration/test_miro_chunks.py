import pytest

from miro_chunks import choose_miro_chunk


@pytest.mark.parametrize("miro_id, expected_miro_chunk", [("A0000001", "A0000000")])
def test_choose_miro_chunk(miro_id, expected_miro_chunk):
    assert choose_miro_chunk(miro_id) == expected_miro_chunk
