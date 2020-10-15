from elasticsearch import Elasticsearch
from elasticsearch import helpers
import boto3
import json
import sys
import os
import shutil
import hashlib
from tqdm import tqdm

#from wellcome_storage_service import StorageServiceClient


def get_aws_resource(resource, *, role_arn):
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]
    return boto3.resource(
        resource,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )


def get_aws_client(resource, *, role_arn):
    assumed_role_object = sts_client.assume_role(
        RoleArn=role_arn, RoleSessionName="AssumeRoleSession1"
    )
    credentials = assumed_role_object["Credentials"]
    return boto3.client(
        resource,
        aws_access_key_id=credentials["AccessKeyId"],
        aws_secret_access_key=credentials["SecretAccessKey"],
        aws_session_token=credentials["SessionToken"],
    )


def get_storage_client(api_url):
    creds_path = os.path.join(
        os.environ["HOME"], ".wellcome-storage", "oauth-credentials.json"
    )
    oauth_creds = json.load(open(creds_path))

    return StorageServiceClient(
        api_url=api_url,
        client_id=oauth_creds["client_id"],
        client_secret=oauth_creds["client_secret"],
        token_url=oauth_creds["token_url"],
    )


def get_secret(secretsmanager_client, secret_id):
    resp = secretsmanager_client.get_secret_value(SecretId=secret_id)

    try:
        # The secret response may be a JSON string of the form
        # {"username": "…", "password": "…", "endpoint": "…"}
        secret = json.loads(resp["SecretString"])
    except ValueError:
        secret = resp["SecretString"]

    return secret


def get_elastic_client(secretsmanager_client, elastic_secret_id):
    secret = get_secret(secretsmanager_client, elastic_secret_id)

    return Elasticsearch(
        secret["endpoint"], http_auth=(secret["username"], secret["password"])
    )


def query_index(elastic_client, elastic_index, elastic_query, transform):
    initial_query = elastic_client.search(
        index=elastic_index,
        body=elastic_query,
        size=0
    )

    document_count = initial_query['hits']['total']['value']
    results = helpers.scan(
        client=elastic_client,
        index=elastic_index,
        size=5,
        query=elastic_query
    )

    with tqdm(total=document_count, file=sys.stdout) as pbar:
        for result in results:
            transform(result)
            sys.exit(1)
            pbar.update(1)


def bag_creator(s3_client, s3_upload_bucket):
    def create_bag(result):
        document = result['_source']

        id = document['id']
        bucket = document['location']['bucket']
        path = document['location']['path']
        version = f"v{document['version']}"

        provider = document['location']['provider']
        assert provider == 'amazon-s3'

        path_prefix = f"s3://{bucket}/{path}/{version}"

        working_id = id.replace("/","_")
        target_folder = "target"
        working_folder = f"{target_folder}/{working_id}"
        tagmanifest_name = "tagmanifest-sha256.txt"
        archive_name = f"{working_id}.tar.gz"
        s3_upload_prefix = "born-digital/archivematica-uuid-update"

        if not os.path.exists(working_folder):
            os.makedirs(working_folder)

        prefix_patterns = [
            "bagit.txt",
            "bag-info.txt",
            "manifest-",
            "tagmanifest-"
        ]

        def _write_fetch_file():
            def _create_fetch_line(file):
                return f"{path_prefix}/{file['path']}\t{file['size']}\t{file['name']}\n"
            files = document['files']

            fetch_file = open(f"{working_folder}/fetch.txt", 'w')
            for file in files:
                fetch_file.write(_create_fetch_line(file))
            fetch_file.close()

        def _filter_bag_files(prefix):
            response = s3_client.list_objects_v2(
                Bucket=bucket,
                Prefix=f"{path}/{version}/{prefix}"
            )

            if 'Contents' in response:
                return [content['Key'] for content in response['Contents']]
            else:
                return []

        def _check_expected_prefixes_exist(file_locations):
            for prefix in prefix_patterns:
                found_match = False

                for location in file_locations:
                    if prefix in location:
                        found_match = True

                if not found_match:
                    raise RuntimeError(f"Missing any files matching prefix: {prefix}")

        def _get_bag_files():
            file_locations = []
            for prefix in prefix_patterns:
                file_locations = file_locations + _filter_bag_files(prefix)

            _check_expected_prefixes_exist(file_locations)

            for location in file_locations:
                filename = location.split("/")[-1]
                save_path = f"{working_folder}/{filename}"
                s3_client.download_file(bucket, location, save_path)

        def _compress_bag():
            shutil.make_archive(working_folder, 'gztar', working_folder)
            shutil.rmtree(working_folder, ignore_errors=True)

        def _generate_checksum(file_location):
            sha256_hash = hashlib.sha256()

            with open(file_location, "rb") as f:
                for byte_block in iter(lambda: f.read(4096), b""):
                    sha256_hash.update(byte_block)

                return sha256_hash.hexdigest()

        def _generate_updated_checksums():
            files_in_need_of_update = [
                'bag-info.txt',
                'fetch.txt'
            ]

            return {filename: _generate_checksum(
                f"{working_folder}/{filename}"
            ) for filename in files_in_need_of_update}

        def _load_existing_checksums():
            tag_manifest = {}

            with open(f"{working_folder}/{tagmanifest_name}") as fp:
                for _, line in enumerate(fp):
                    split_manifest_line = line.split(" ")
                    checksum = split_manifest_line[0].strip()
                    filename = split_manifest_line[1].strip()

                    assert checksum
                    assert filename

                    tag_manifest[filename] = checksum

            return tag_manifest

        def _write_new_tagmanifest():
            existing_checksums = _load_existing_checksums()
            new_checksums = _generate_updated_checksums()

            merged_checksums = dict(existing_checksums.items() | new_checksums.items())

            with open(f"{working_folder}/{tagmanifest_name}", "w") as fp:
                for checksum, filename in merged_checksums.items():
                    fp.write(f"{filename} {checksum}\n")

        def _update_bag_info():
            # Get Archivematica UUID ???
            # Add Archivematica UUID to bag-info.txt as Internal-Sender-Identifier
            with open(f"{working_folder}/bag-info.txt", "a") as fp:
                fp.write("Internal-Sender-Identifier: nope")

        def _upload_bag_to_s3():
            s3_client.upload_file(
                f"{working_folder}/{archive_name}",
                s3_upload_bucket,
                f"{s3_upload_prefix}/{archive_name}"
            )

        _write_fetch_file()
        _get_bag_files()
        _update_bag_info()
        _write_new_tagmanifest()
        _upload_bag_to_s3()

        sys.exit(1)

        _compress_bag()
        # Upload bag to S3
        _upload_bag_to_s3()
        # Ingest bag into storage service using client lib using correct space
        #   - born-digital or born-digital-accessions

    return create_bag


if __name__ == "__main__":
    role_arn = 'arn:aws:iam::975596993436:role/storage-developer'
    elastic_secret_id = 'archivematica_bags_migration/credentials'
    index = 'storage_bags'

    environment_id = 'stage'

    environments = {
        "prod": {
            "bucket": "wellcomecollection-archivematica-ingests",
            "api_url": "https://api.wellcomecollection.org/storage/v1"
        },
        "stage": {
            "bucket": "wellcomecollection-archivematica-staging-ingests",
            "api_url": "https://api-stage.wellcomecollection.org/storage/v1"

        }
    }

    api_url = environments[environment_id]['api_url']

    sts_client = boto3.client("sts")

    secretsmanager_client = get_aws_client(
        resource='secretsmanager',
        role_arn=role_arn
    )

    s3_client = get_aws_client(
        resource='s3',
        role_arn=role_arn
    )

    elastic_client = get_elastic_client(
        secretsmanager_client=secretsmanager_client,
        elastic_secret_id=elastic_secret_id
    )

    #storage_client = get_storage_client(api_url=api_url)

    # TODO: update query to include born-digital-accessions
    query = {
        "query": {
            "term": {
                "space": {
                    "value": "born-digital"
                }
            }
        }
    }

    s3_upload_bucket = environments[environment_id]['bucket']
    transform = bag_creator(s3_client, s3_upload_bucket)

    query_index(elastic_client, index, query, transform)
