import datetime
import json
import sys

from wellcome_storage_service import IngestNotFound, prod_client, staging_client

from helpers.iam import READ_ONLY_ROLE_ARN, create_aws_client_from_role_arn
from helpers.s3 import list_s3_prefix


def lookup_ingest(ingest_id):
    """
    Looks up an ingest in both APIs

    If it finds an ingest, it returns a tuple:

        (API name, API client, ingest_data)

    If it doesn't find an ingest, it returns IngestNotFound.

    """
    api_variants = {"staging": staging_client(), "prod": prod_client()}

    for api_name, client in api_variants.items():
        try:
            ingest = client.get_ingest(ingest_id)
        except IngestNotFound:
            pass
        else:
            ingest_data = {
                "space": ingest["space"]["id"],
                "external_identifier": ingest["bag"]["info"]["externalIdentifier"],
                "version": ingest["bag"]["info"]["version"],
                "date_created": datetime.datetime.strptime(
                    ingest["createdDate"], "%Y-%m-%dT%H:%M:%S.%fZ"
                ),
            }
            return api_name, client, ingest_data

    raise IngestNotFound


def lookup_ingest_id(environment, space, external_identifier, version):
    if environment == "staging":
        manifests_bucket = "wellcomecollection-vhs-storage-staging-manifests"
    elif environment == "prod":
        manifests_bucket = "wellcomecollection-vhs-storage-manifests"
    else:
        sys.exit(f"Unrecognised environment: {environment}")

    s3 = create_aws_client_from_role_arn("s3", role_arn=READ_ONLY_ROLE_ARN)

    matching_manifests = list(
        list_s3_prefix(
            s3,
            bucket=manifests_bucket,
            prefix=f"{space}/{external_identifier}/{version.replace('v', '')}/",
        )
    )

    if len(matching_manifests) >= 1:
        manifest_key = matching_manifests[0]

        manifest = json.load(
            s3.get_object(Bucket=manifests_bucket, Key=manifest_key)["Body"]
        )

        return manifest["ingestId"]
    else:
        sys.exit(
            f"Unable to find ingest ID from manifests bucket! ({len(matching_manifests)} manifests found)"
        )
