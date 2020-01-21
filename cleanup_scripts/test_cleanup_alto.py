import shutil

import cleanup_alto


def test_getting_alto_paths():
    assert list(cleanup_alto.get_alto_paths("examples")) == [
        ("b31366752_0004_0001.xml", "examples/b31366752_alto/b31366752_0004_0001.xml"),
        ("b31366752_0004_0003.xml", "examples/b31366752_alto/b31366752_0004_0003.xml"),
        ("b31366752_0004_0004.xml", "examples/b31366752_alto/b31366752_0004_0004.xml"),
        ("b31366752_not_in_manifest.xml", "examples/b31366752_alto/b31366752_not_in_manifest.xml"),
        ("nobnumber.xml", "examples/b31366752_alto/nobnumber.xml"),
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

    assert for_deletion == [
        ("good.xml", 100),
    ]

    output = capsys.readouterr()
    assert "Sizes don't match:" in output.err


def test_get_paths_for_deletion_ignores_mismatched_checksum(capsys):
    info_blobs = [
        {
            "path": "bad.xml",
            "alto_size": 100,
            "stored_size": 100,
            "alto_checksum": "123",
            "stored_checksum": "456",
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

    assert for_deletion == [("good.xml", 100)]

    output = capsys.readouterr()
    assert "Checksums don't match:" in output.err


def test_can_get_bag():
    bag1 = cleanup_alto.get_bag(b_number="b10109377")
    assert bag1["type"] == "Bag"

    bag2 = cleanup_alto.get_bag(b_number="b10109377")

    assert bag1 == bag2


def test_can_get_xml_files():
    files = cleanup_alto.get_xml_files(b_number="b31366752")

    assert len(files) == 1913

    assert "b31366752_0004_0001.xml" in files
    assert files["b31366752_0004_0001.xml"] == {
        "checksum": "aae9777ef9fcb264bca73b093b152bcd9b20495378604514de72315a2e7ecd45",
        "size": 3342,
    }


def test_checksum():
    assert (
        cleanup_alto.checksum("examples/b31366752_alto/b31366752_0004_0003.xml")
        == "a16be8aa883485cf040cb8876324bd2199f42da91b6d03dba3b3c3eb8c816732"
    )


def test_size():
    assert cleanup_alto.size("examples/b31366752_alto/b31366752_0004_0003.xml") == 36367


def test_get_alto_blobs(capsys):
    paths = cleanup_alto.get_alto_paths("examples")
    info_blobs = cleanup_alto.get_info_blobs(paths)

    assert list(info_blobs) == [
        {
            "name": "b31366752_0004_0001.xml",
            "path": "examples/b31366752_alto/b31366752_0004_0001.xml",
            "b_number": "b31366752",
            "alto_checksum": "f6df3bf9106476c893023b5d1485e3df6565b83d3eebd696d13b178717f34f29",
            "alto_size": 3342,
            "stored_checksum": "aae9777ef9fcb264bca73b093b152bcd9b20495378604514de72315a2e7ecd45",
            "stored_size": 3342,
        },
        {
            "name": "b31366752_0004_0003.xml",
            "path": "examples/b31366752_alto/b31366752_0004_0003.xml",
            "b_number": "b31366752",
            "alto_checksum": "a16be8aa883485cf040cb8876324bd2199f42da91b6d03dba3b3c3eb8c816732",
            "alto_size": 36367,
            "stored_checksum": "a16be8aa883485cf040cb8876324bd2199f42da91b6d03dba3b3c3eb8c816732",
            "stored_size": 36367,
        },
        {
            "name": "b31366752_0004_0004.xml",
            "path": "examples/b31366752_alto/b31366752_0004_0004.xml",
            "b_number": "b31366752",
            "alto_checksum": "d0df535809e2acc53658a64997c805120df3df481d963be5fa8ef14d2d696dd5",
            "alto_size": 49,
            "stored_checksum": "285424d29f8b98cec0c354f44f5bb1e2e392648f273f8419e5c6844107765f88",
            "stored_size": 1594,
        },
    ]

    output = capsys.readouterr()
    assert (
        "Unable to find file in storage manifest for %r"
        % "examples/b31366752_alto/b31366752_not_in_manifest.xml"
    ) in output.err
    assert (
        "Unable to identify b number in %r" % "examples/b31366752_alto/nobnumber.xml"
    ) in output.err


def test_deleter(tmpdir):
    shutil.copytree("examples", tmpdir / "examples")

    cleanup_alto.run_deleter(tmpdir / "examples")

    assert (tmpdir / "examples" / "alto" / "b31366752_0004_0003.xml").exists()
    assert (tmpdir / "examples" / "b31366752_alto" / "b31366752_0004_0001.xml").exists()
    assert not (tmpdir / "examples" / "b31366752_alto" / "b31366752_0004_0034.xml").exists()
    assert (tmpdir / "examples" / "b31366752_alto" / "b31366752_0004_0004.xml").exists()
    assert (tmpdir / "examples" / "b31366752_alto" / "b31366752_not_in_manifest.xml").exists()
    assert (tmpdir / "examples" / "b31366752_alto" / "nobnumber.xml").exists()
    assert (tmpdir / "examples" / "b31366752_alto" / "notalto.md").exists()
    assert (tmpdir / "examples" / "notalto" / "hello.txt").exists()
