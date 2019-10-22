# -*- encoding: utf-8

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

        with pytest.raises(
            Exception,
            match='Could not connect to the endpoint URL: "https://does-not-exist.s3.amazonaws.com'
        ):
            downloader.download_bag(bag, out_dir=str(tmpdir))

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
            "An error occurred (404) when calling the HeadObject operation"
        )
