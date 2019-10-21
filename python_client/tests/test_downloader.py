# -*- encoding: utf-8

import pytest

from wellcome_storage_service.downloader import download_bag


def test_cannot_download_a_bag_with_wrong_provider(tmpdir):
    bag = {
        "location": {"provider": {"id": "nope"}},
        "manifest": {"files": []},
        "tagManifest": {"files": []},
    }

    with pytest.raises(RuntimeError, match="Unsupported storage provider: nope"):
        download_bag(bag, out_dir=tmpdir)
