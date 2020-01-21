import cleanup_alto


def test_getting_alto_paths():
    assert list(cleanup_alto.get_alto_paths("examples")) == [
        ("b31366752_0004_0003.xml", "examples/b31366752_alto/b31366752_0004_0003.xml")
    ]


def test_get_paths_for_deletion_ignores_mismatched_size(capsys):
    info_blobs = [
        {
            "path": "bad.xml",
            "alto_size": 100,
            "stored_size": 200,
            "alto_checksum": "123",
            "stored_checksum": "123",
        },
        {
            "path": "good.xml",
            "alto_size": 100,
            "stored_size": 100,
            "alto_checksum": "123",
            "stored_checksum": "123",
        },
    ]

    for_deletion = list(cleanup_alto.get_paths_for_deletion(info_blobs))

    assert for_deletion == [info_blobs[1]]

    output = capsys.readouterr()
    assert "Sizes don't match:" in output.err
