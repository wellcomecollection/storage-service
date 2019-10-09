# -*- encoding: utf-8

import dateutil.parser as dp
import re


class BibNumberGenerator:
    def __init__(
        self,
        s3_client,
        mets_bucket_name="wellcomecollection-assets-workingstorage",
        mets_only_root_prefix="mets_only/",
    ):
        self.s3_client = s3_client
        self.bucket = mets_bucket_name
        self.prefix = mets_only_root_prefix

        self.bnumber_pattern = re.compile(
            r"\A" + self.prefix + r"[0-9ax/]*/(b[0-9ax]{8}).xml\Z"
        )

    def _get_matching_s3_objects(self, bucket, prefix="", suffix=""):
        kwargs = {"Bucket": bucket}

        if isinstance(prefix, str):
            kwargs["Prefix"] = prefix

        while True:
            resp = self.s3_client.list_objects_v2(**kwargs)

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

    def _get_matching_s3_keys(self, bucket, prefix="", suffix=""):
        for obj in self._get_matching_s3_objects(bucket, prefix, suffix):
            yield obj["Key"]

    def get(self, bnumber):
        shard_path = "/".join(list(bnumber[-4:][::-1]))
        key = f"{self.prefix}{shard_path}/{bnumber}.xml"

        response = self.s3_client.head_object(Bucket=self.bucket, Key=key)

        headers = response["ResponseMetadata"]["HTTPHeaders"]

        return {
            "bnumber": bnumber,
            "key": key,
            "last_modified": dp.parse(headers["last-modified"]).isoformat(),
            "content_type": headers["content-type"],
            "content_length": int(headers["content-length"]),
        }

    def bnumbers(self):
        for key in self._get_matching_s3_keys(self.bucket, self.prefix):
            match = self.bnumber_pattern.match(key)

            if match:
                yield match.group(1)
