import re

import boto3

class BibNumberGenerator:
    def __init__(self, s3_client, mets_bucket_name='wellcomecollection-assets-workingstorage', mets_only_root_prefix='mets_only/'):
        self.s3_client = s3_client
        self.bucket = mets_bucket_name
        self.prefix = mets_only_root_prefix

    def _get_matching_s3_objects(self, client, bucket, prefix='', suffix=''):
        kwargs = {"Bucket": bucket}

        if isinstance(prefix, str):
            kwargs["Prefix"] = prefix

        while True:
            resp = client.list_objects_v2(**kwargs)

            try:
                contents = resp["Contents"]
            except KeyError:
                return

            for obj in contents:
                key = obj["Key"]
                if key.startswith(prefix) and key.endswith(suffix):
                    yield obj

            try:
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]
            except KeyError:
                break

    def _get_matching_s3_keys(self, client, bucket, prefix="", suffix=""):
        for obj in self._get_matching_s3_objects(client, bucket, prefix, suffix):
            yield obj["Key"]

    def bnumbers(self):
        bucket = self.bucket
        prefix = self.prefix

        bnumber_pattern = re.compile(
            r"\A" + prefix + r"[0-9ax/]*/(b[0-9ax]{8}).xml\Z"
        )

        for key in self._get_matching_s3_keys(self.s3_client, self.bucket, self.prefix):
            match = bnumber_pattern.match(key)
            if bnumber_pattern.match(key):
                yield match.group(1)