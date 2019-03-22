#!/usr/bin/env python
# -*- encoding: utf-8

from wellcome_storage_service import StorageServiceClient


def test_can_create_client():
    client = StorageServiceClient(api_url="http://example.org", sess=None)
