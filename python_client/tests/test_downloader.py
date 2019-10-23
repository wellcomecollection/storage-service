# -*- encoding: utf-8

import tarfile

from botocore.exceptions import ClientError
import pytest

from wellcome_storage_service import downloader


def test_cannot_download_a_bag_with_wrong_provider(tmpdir):
    bag = {
        "location": {"provider": {"id": "nope"}},
        "manifest": {"files": []},
        "tagManifest": {"files": []},
    }

    with pytest.raises(RuntimeError, match="Unsupported storage provider: nope"):
        downloader.download_bag(bag, out_dir=tmpdir)


class TestS3IADownload(object):
    # Implementation note: these tests are reading from the production bucket.
    #
    # This isn't ideal, but it was simpler than trying to set up a Docker image,
    # create a bucket, intercept client creation in downloader.py…
    #
    # This makes the tests quite slow -- if we end up editing this code a lot,
    # refactoring these tests to use a local source would be sensible.

    def test_downloading_bag(self, tmpdir):
        bag = {
            "location": {
                "provider": {"id": "aws-s3-ia"},
                "bucket": "wellcomecollection-storage",
                "path": "digitised/b10002819",
            },
            "manifest": {
                "files": [
                    {"name": "data/b10002819.xml", "path": "v1/data/b10002819.xml"}
                ]
            },
            "tagManifest": {
                "files": [
                    {"name": "bagit.txt", "path": "v1/bagit.txt"},
                    {"name": "manifest-sha256.txt", "path": "v1/manifest-sha256.txt"},
                ]
            },
        }

        downloader.download_bag(bag, out_dir=str(tmpdir))

        assert tmpdir.join("bagit.txt").read() == (
            "BagIt-Version: 0.97\nTag-File-Character-Encoding: UTF-8\n"
        )

        assert tmpdir.join("manifest-sha256.txt").exists()
        assert tmpdir.join("data/b10002819.xml").exists()

    def test_downloading_compressed_bag(self, tmpdir):
        # This test case is based on a real bag, b11733330, because it's
        # only 1.3MB in size, and we can download the whole thing.
        out_path = tmpdir.join("b11733330.tar.gz")

        bag = {
            "location": {
                "bucket": "wellcomecollection-storage",
                "path": "digitised/b11733330",
                "provider": {"id": "aws-s3-ia", "type": "Provider"},
            },
            "manifest": {
                "files": [
                    {
                        "name": "data/b11733330.xml",
                        "path": "v1/data/b11733330.xml",
                        "size": 8132,
                    },
                    {
                        "name": "data/objects/L0008210-LS-CS.jp2",
                        "path": "v1/data/objects/L0008210-LS-CS.jp2",
                        "size": 1327244,
                    },
                ]
            },
            "tagManifest": {
                "files": [
                    {
                        "name": "manifest-sha256.txt",
                        "path": "v1/manifest-sha256.txt",
                        "size": 183,
                    },
                    {"name": "bagit.txt", "path": "v1/bagit.txt", "size": 55},
                    {
                        "name": "manifest-sha512.txt",
                        "path": "v1/manifest-sha512.txt",
                        "size": 311,
                    },
                    {"name": "bag-info.txt", "path": "v1/bag-info.txt", "size": 331},
                ]
            }
        }

        downloader.download_compressed_bag(bag, out_path=str(out_path))

        files = {
            "data/b11733330.xml": 8132,
            "data/objects/L0008210-LS-CS.jp2": 1327244,
            "manifest-sha256.txt": 183,
            "bagit.txt": 55,
            "manifest-sha512.txt": 311,
            "bag-info.txt": 331,
        }

        with tarfile.open(str(out_path), "r:gz") as tf:
            members = tf.getmembers()
            assert len(members) == len(files)

            for tarinfo in members:
                assert files[tarinfo.name] == tarinfo.size

            bagit = tf.getmember("bagit.txt")
            tf.extract(bagit, path=str(tmpdir))
            assert tmpdir.join("bagit.txt").read() == (
                "BagIt-Version: 0.97\n" "Tag-File-Character-Encoding: UTF-8\n"
            )

    def test_downloading_from_non_existent_bucket_is_error(self, tmpdir):
        bag = {
            "location": {
                "provider": {"id": "aws-s3-ia"},
                "bucket": "does-not-exist",
                "path": "digitised/b10002819",
            },
            "manifest": {
                "files": [
                    {"name": "data/b10002819.xml", "path": "v1/data/b10002819.xml"}
                ]
            },
            "tagManifest": {"files": []},
        }

        with pytest.raises(Exception) as err:
            downloader.download_bag(bag, out_dir=str(tmpdir))

        assert err.value.args[0].startswith(
            (
                'Could not connect to the endpoint URL: "https://does-not-exist.s3.amazonaws.com',
                "An error occurred (403) when calling the HeadObject operation",
                "An error occurred (AccessDenied) when calling the GetObject operation",
            )
        )

    def test_downloading_from_non_existent_key_is_error(self, tmpdir):
        bag = {
            "location": {
                "provider": {"id": "aws-s3-ia"},
                "bucket": "wellcomecollection-storage",
                "path": "does-not-exist/b10002819",
            },
            "manifest": {
                "files": [
                    {"name": "data/b10002819.xml", "path": "v1/data/b10002819.xml"}
                ]
            },
            "tagManifest": {"files": []},
        }

        with pytest.raises(ClientError) as err:
            downloader.download_bag(bag, out_dir=str(tmpdir))

        assert err.value.args[0].startswith(
            (
                "An error occurred (404) when calling the HeadObject operation",
                "An error occurred (NoSuchKey) when calling the GetObject operation",
            )
        )
