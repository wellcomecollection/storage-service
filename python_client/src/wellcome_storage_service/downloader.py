# -*- encoding: utf-8

import abc
import os

try:
    from collections.abc import ABC
except ImportError:  # Python 2
    from abc import ABCMeta as ABC

from ._utils import mkdir_p


def download_bag(storage_manifest, out_dir):
    """
    Download all the files in a bag to a given directory.

    :param storage_manifest: A storage manifest returned from the storage
        service, as retrieved with ``get_bag()``.
    :param out_dir: The directory to download the bag to.

    """
    all_files = (
        storage_manifest["manifest"]["files"] + storage_manifest["tagManifest"]["files"]
    )

    location = storage_manifest["location"]

    if location["provider"]["id"] == "aws-s3-ia":
        provider = S3InfrequentAccessProvider()
    else:
        raise RuntimeError(
            "Unsupported storage provider: %s" % location["provider"]["id"]
        )

    for manifest_file in all_files:
        provider.download(
            out_dir=out_dir, location=location, manifest_file=manifest_file
        )


class AbstractProvider(object):
    """
    Abstract class for a downloader.

    Subclasses should implement the ``_download_fileobj`` method, which
    downloads a single ``manifest_file`` from a bag's manifest at ``location``
    to a writable ``file_obj`` in binary mode.
    """

    __metaclass__ = ABC

    @abc.abstractmethod
    def _download_fileobj(self, location, manifest_file, file_obj):
        pass

    def download(self, out_dir, location, manifest_file):
        out_path = os.path.join(out_dir, manifest_file["name"])

        mkdir_p(os.path.dirname(out_path))

        with open(out_path, "wb") as file_obj:
            self._download_fileobj(
                location=location, manifest_file=manifest_file, file_obj=file_obj
            )


class S3InfrequentAccessProvider(AbstractProvider):

    def __init__(self):
        import boto3

        self.s3_client = boto3.client("s3")
        super(S3InfrequentAccessProvider, self).__init__()

    def _download_fileobj(self, location, manifest_file, file_obj):
        assert location["provider"]["id"] == "aws-s3-ia"

        bucket = location["bucket"]
        path_prefix = location["path"]

        s3_key = os.path.join(path_prefix, manifest_file["path"])

        self.s3_client.download_fileobj(Bucket=bucket, Key=s3_key, Fileobj=file_obj)
