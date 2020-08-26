#!/usr/bin/env python
import json
import uuid
from botocore.exceptions import ClientError
from deepdiff import DeepDiff

from common import get_aws_resource, scan_table

DEVELOPER_ROLE_ARN = "arn:aws:iam::975596993436:role/storage-developer"
dynamodb = get_aws_resource("dynamodb", role_arn=DEVELOPER_ROLE_ARN)
s3 = get_aws_resource("s3", role_arn=DEVELOPER_ROLE_ARN)

vhs_table = "vhs-storage-staging-manifests-2020-07-24"

backfill_vhs_table = "vhs-storage-staging-manifests-2020-08-19"

errors = {}


def record_error(id, version, err):
    print(f"\033[91mError while updating {id}/{version}: {err}\u001b[0m")
    errors[f"{id}/{version}"] = err


def get_bucket_key(item):
    try:
        bucket = item["payload"]["namespace"]
        key = item["payload"]["path"]
    except KeyError:
        try:
            bucket = item["payload"]["bucket"]
            key = item["payload"]["key"]
        except:
            record_error(
                item["id"], item["version"], f"Cannot find s3 bucket, key in {item}"
            )
        else:
            return bucket, key
    else:
        return bucket, key


def get_vhs_json(id, version, bucket, key):
    try:
        return json.loads(s3.Object(bucket, key).get()["Body"].read().decode("utf-8"))
    except ClientError as e:
        record_error(id, version, f"Cannot read s3 entry for {id}: {item}: {e}")


def put_vhs_json(id, version, bucket, item, content):
    filename = f"{uuid.uuid4()}.json"
    key = f"{id}/{version}/{filename}"
    item["payload"] = {"bucket": bucket, "key": key}
    try:
        s3.Object(bucket, key).put(Body=(bytes(json.dumps(content).encode("UTF-8"))))
        dynamodb.Table(vhs_table).put_item(Item=item)
    except ClientError as e:
        record_error(
            id,
            version,
            f"Error updating backfill vhs object {id}/{version} to s3://{bucket}/{key}: {e}",
        )


def get_backfill_item(id, version):
    try:
        return dynamodb.Table(backfill_vhs_table).get_item(
            Key={"id": id, "version": version}
        )["Item"]
    except KeyError:
        record_error(id, version, f"Cannot find backfill storage manifest!!!!")


def is_expected_diff(id, version, diff):
    if diff:
        values_changed = diff.pop("values_changed", None)
        items_added = diff.pop("iterable_item_added", None)
        if values_changed and values_changed.keys() - ["root['createdDate']"]:
            record_error(id, version, f"Unexpected values changed in {values_changed}!")
            return False
        if items_added and items_added.keys() - ["root['replicaLocations'][1]"]:
            record_error(id, version, f"Unexpected values changed in {items_added}!")
            return False
        if diff:
            record_error(id, version, f"Unexpected values changed in {diff}!")
            return False
    return True


for item in scan_table(TableName=vhs_table):
    id = item["id"]
    version = item["version"]
    print(f"Updating {id}/{version}")
    bucket_key = get_bucket_key(item)
    if bucket_key:
        bucket, key = bucket_key
        vhs_content = get_vhs_json(id, version, bucket, key)
        if vhs_content and len(vhs_content['replicaLocations']) < 2:
            created_date = vhs_content["createdDate"]
            backfilled_item = get_backfill_item(id, version)
            if backfilled_item:
                backfilled_bucket = backfilled_item["payload"]["bucket"]
                backfilled_key = backfilled_item["payload"]["key"]
                backfilled_json = get_vhs_json(
                    id, version, backfilled_bucket, backfilled_key
                )
                if backfilled_json:
                    diff = DeepDiff(vhs_content, backfilled_json, ignore_order=True)
                    if diff and is_expected_diff(id, version, diff):
                        backfilled_json["createdDate"] = created_date
                        put_vhs_json(
                            id, version, bucket, backfilled_item, backfilled_json
                        )
if errors:
    print("\033[91mThere are errors!")
    exit(1)
else:
    print("\033[92mFinished with no errors!")
    exit(0)
