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
            r"\A" + self.prefix + r"[0-9ax/]*/(?P<bnumber>b[0-9ax]{8}).xml\Z"
        )

    def get_bnumber(self, s):
        try:
            return self.bnumber_pattern.match(s).group("bnumber")
        except AttributeError:
            raise ValueError(f"Cannot find b number in {s!r}")

    def _get_matching_s3_objects(self, bucket, prefix="", start_after=None):
        kwargs = {"Bucket": bucket}

        if isinstance(prefix, str):
            kwargs["Prefix"] = prefix

        if start_after is not None:
            kwargs["StartAfter"] = start_after

        while True:
            resp = self.s3_client.list_objects_v2(**kwargs)

            try:
                contents = resp["Contents"]
            except KeyError:
                return

            for obj in contents:
                key = obj["Key"]
                if key.startswith(prefix):
                    yield obj

            try:
                kwargs["ContinuationToken"] = resp["NextContinuationToken"]
            except KeyError:
                break

    def _get_matching_s3_keys(self, bucket, prefix=""):
        for obj in self._get_matching_s3_objects(bucket, prefix):
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

    def bnumber_objects(self, first_bnumber):
        shard_path = "/".join(list(first_bnumber[-4:][::-1]))
        key = f"{self.prefix}{shard_path}/{first_bnumber}.xml"

        for obj in self._get_matching_s3_objects(
            bucket=self.bucket, prefix=self.prefix, start_after=key
        ):
            try:
                bnumber = self.get_bnumber(obj["Key"])
            except ValueError:
                pass
            else:
                yield {
                    "bnumber": bnumber,
                    "key": obj["Key"],
                    "last_modified": obj["LastModified"].isoformat(),
                    "content_length": obj["Size"],
                }

    def bnumbers(self):
        for key in self._get_matching_s3_keys(self.bucket, self.prefix):
            try:
                yield self.get_bnumber(obj["Key"])
            except ValueError:
                pass
