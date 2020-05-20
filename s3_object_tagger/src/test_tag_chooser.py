import pytest

from tag_chooser import choose_tags


@pytest.mark.parametrize(
    "key, expected_tags",
    [
        # MXF video masters get a tag
        ("digitised/b1234/v1/cats.mxf", [("FileType", "MXF video master")]),
        # If the file extension is uppercase, it gets tagged
        ("digitised/b1234/v1/cats.MXF", [("FileType", "MXF video master")]),
        # Files with a different extension don't get tagged
        ("digitised/b1234/v1/cats.mp4", []),
        # MXF files in a different space don't get tagged
        ("born-digital/CA/TS/v1/cats.MXF", []),
    ],
)
def test_choose_tags(key, expected_tags):
    assert choose_tags(bucket="wellcomecollection-example", key=key) == expected_tags
