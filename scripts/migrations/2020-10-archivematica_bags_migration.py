#!/usr/bin/env python3
"""
This is a script to append an Archivematica UUID as
an Internal-Sender-Identifier to existing born-digital bags.

When run this will create a "target" folder in the same directory
to work within. It will leave "some_bag_id.log" files for each
bag it migrates.
"""

import datetime
import hashlib
import os
import shutil
import sys

from elasticsearch import helpers
from tqdm import tqdm

from common import get_aws_resource, get_aws_client, get_storage_client, get_secret, get_elastic_client


def generate_checksum(file_location):
    sha256_hash = hashlib.sha256()

    with open(file_location, "rb") as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)

        return sha256_hash.hexdigest()


def compress_folder(folder, remove_folder=True):
    archive_name = shutil.make_archive(folder, 'gztar', folder)
    if remove_folder:
        shutil.rmtree(folder, ignore_errors=True)

    return archive_name


def filter_s3_objects(s3_client, bucket, prefix):
    response = s3_client.list_objects_v2(
        Bucket=bucket,
        Prefix=prefix
    )

    if 'Contents' in response:
        return [content['Key'] for content in response['Contents']]
    else:
        return []


def load_space_separated_file(file_location, key_first=True):
    fields = {}

    with open(file_location) as fp:
        for line in fp:
            split_line = line.split(" ")
            first = split_line[0].strip()
            second = split_line[1].strip()

            assert first
            assert second

            if key_first:
                fields[first] = second
            else:
                fields[second] = first

    return fields


class ArchivematicaUUIDBagMigrator:
    def __init__(self, workflow_s3_client, storage_s3_client, storage_client, s3_upload_bucket):
        self.workflow_s3_client = workflow_s3_client
        self.storage_s3_client = storage_s3_client
        self.storage_client = storage_client
        self.s3_upload_bucket = s3_upload_bucket

        self.target_folder = "target"
        self.tagmanifest_name = "tagmanifest-sha256.txt"
        self.s3_upload_prefix = "born-digital/archivematica-uuid-update"

    @staticmethod
    def _get_archivematica_uuid(bucket, path, version):
        files = filter_s3_objects(
            s3_client=storage_s3_client,
            bucket=bucket,
            prefix=f"{path}/{version}/data/METS."
        )

        assert len(files) == 1, files
        mets_file_with_id = files[0]

        archivematica_uuid = (
            mets_file_with_id.split('/METS.')[-1].split('.xml')[0]
        )

        assert archivematica_uuid
        return archivematica_uuid

    @staticmethod
    def _generate_updated_checksums(working_folder):
        files_in_need_of_update = [
            'bag-info.txt',
            'fetch.txt'
        ]

        return {filename: generate_checksum(
            f"{working_folder}/{filename}"
        ) for filename in files_in_need_of_update}

    def _load_existing_checksums(self, working_folder):
        tag_manifest = load_space_separated_file(
            file_location=f"{working_folder}/{self.tagmanifest_name}",
            key_first=False
        )

        files_that_should_be_referenced = [
            'bag-info.txt',
            'bagit.txt',
            'manifest-sha256.txt'
        ]

        assert any(filename in files_that_should_be_referenced for filename in tag_manifest.keys())

        return tag_manifest

    @staticmethod
    def _write_fetch_file(bucket, path, working_folder, files):
        path_prefix = f"s3://{bucket}/{path}"

        with open(f"{working_folder}/fetch.txt", "w") as fetch_file:
            for file in files:
                s3_uri = f"{path_prefix}/{file['path']}"
                fetch_file.write(f"{s3_uri}\t{file['size']}\t{file['name']}\n")

    @staticmethod
    def _get_bagit_files_from_s3(bucket, path, version, working_folder):
        prefix_patterns = [
            "bagit.txt",
            "bag-info.txt",
            "manifest-",
            "tagmanifest-"
        ]

        file_locations = []
        for prefix in prefix_patterns:
            files_with_prefix = filter_s3_objects(
                s3_client=storage_s3_client,
                bucket=bucket,
                prefix=f"{path}/{version}/{prefix}"
            )

            file_locations = file_locations + files_with_prefix

        for prefix in prefix_patterns:
            if not any(prefix in location for location in file_locations):
                raise RuntimeError(f"Missing any files matching prefix: {prefix}")

        for location in file_locations:
            filename = location.split("/")[-1]
            save_path = f"{working_folder}/{filename}"
            storage_s3_client.download_file(bucket, location, save_path)

    @staticmethod
    def _append_archivematica_uuid(working_folder, archivematica_uuid):
        bag_info = load_space_separated_file(
            file_location=f"{working_folder}/bag-info.txt"
        )

        if 'Internal-Sender-Identifier:' in bag_info:
            assert bag_info['Internal-Sender-Identifier:'] == archivematica_uuid, archivematica_uuid
            return False
        else:
            with open(f"{working_folder}/bag-info.txt", "a") as fp:
                fp.write(f"Internal-Sender-Identifier: {archivematica_uuid}\n")
            return True

    def _update_tagmanifest(self, working_folder):
        existing_checksums = self._load_existing_checksums(working_folder)
        new_checksums = self._generate_updated_checksums(working_folder)

        merged_checksums = dict(existing_checksums)
        for k,v in new_checksums.items():
            merged_checksums[k] = v

        assert 'fetch.txt' in merged_checksums.keys()

        old_bag_info_checksum = existing_checksums.get('bag-info.txt')
        new_bag_info_checksum = merged_checksums.get('bag-info.txt')

        assert old_bag_info_checksum != new_bag_info_checksum

        with open(f"{working_folder}/{self.tagmanifest_name}", "w") as fp:
            for checksum, filename in merged_checksums.items():
                fp.write(f"{filename} {checksum}\n")

    def _upload_bag_to_s3(self, archive_location, working_id, remove_bag=True):
        s3_upload_key = f"{self.s3_upload_prefix}/{working_id}.tar.gz"
        workflow_s3_client.upload_file(
            Filename=archive_location,
            Bucket=s3_upload_bucket,
            Key=s3_upload_key
        )
        if remove_bag:
            os.remove(archive_location)

        return s3_upload_key

    def migrate(self, storage_manifest):
        id = storage_manifest['id']
        bucket = storage_manifest['location']['bucket']
        path = storage_manifest['location']['path']
        files = storage_manifest['files']
        version = f"v{storage_manifest['version']}"
        space = storage_manifest['space']
        external_identifier = storage_manifest['info']['externalIdentifier']
        provider = storage_manifest['location']['provider']

        assert provider == 'amazon-s3'

        working_id = id.replace("/", "_")
        working_folder = f"{self.target_folder}/{working_id}"

        os.makedirs(working_folder, exist_ok=True)

        # Initialise working log
        with open(f"{self.target_folder}/{working_id}.log", "w") as fp:
            fp.write(f"{datetime.datetime.now().isoformat()}: Starting migration for {id}\n")

        def _log(msg):
            with open(f"{self.target_folder}/{working_id}.log", "a") as fp:
                fp.write(f"{datetime.datetime.now().isoformat()}: {msg}\n")

        # Write fetch.txt
        self._write_fetch_file(
            working_folder=working_folder,
            bucket=bucket,
            path=path,
            files=files
        )
        _log(f"Wrote fetch.txt")

        # Get required files from bag
        self._get_bagit_files_from_s3(
            working_folder=working_folder,
            bucket=bucket,
            path=path,
            version=version,
        )
        _log(f"Got BagIt files from S3")

        # Update bag-info.txt
        archivematica_uuid = self._get_archivematica_uuid(
            bucket=bucket,
            path=path,
            version=version
        )

        did_append_uuid = self._append_archivematica_uuid(working_folder, archivematica_uuid)
        if not did_append_uuid:
            _log(f"Internal-Sender-Identifier found in bag-info.txt: {archivematica_uuid}")
            _log(f"Not migrating {id} (already migrated)")
            return
        else:
            _log(f"Appended Internal-Sender-Identifier to bag-info.txt: {archivematica_uuid}")

        # Update tagmanifest-sha256.txt
        self._update_tagmanifest(
            working_folder=working_folder
        )
        _log(f"Updated {self.tagmanifest_name}")

        # Create compressed bag
        archive_location = compress_folder(
            folder=working_folder
        )
        _log(f"Created archive: {archive_location}")

        # Upload compressed bag to S3
        s3_upload_key = self._upload_bag_to_s3(
            archive_location=archive_location,
            working_id=working_id
        )
        _log(f"Uploaded bag to s3://{s3_upload_bucket}/{s3_upload_key}")

        # Request ingest of uploaded bag from Storage Service
        ingest_uri = storage_client.create_s3_ingest(
            space=space,
            external_identifier=external_identifier,
            s3_bucket=s3_upload_bucket,
            s3_key=s3_upload_key,
            ingest_type="update"
        )
        _log(f"Requested ingest: {ingest_uri}")
        _log(f"Completed migration for {id}")


if __name__ == "__main__":
    storage_role_arn = 'arn:aws:iam::975596993436:role/storage-developer'
    workflow_role_arn = 'arn:aws:iam::299497370133:role/workflow-developer'

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

    workflow_s3_client = get_aws_client(
        resource='s3',
        role_arn=workflow_role_arn
    )

    storage_s3_client = get_aws_client(
        resource='s3',
        role_arn=storage_role_arn
    )

    elastic_client = get_elastic_client(
        role_arn=storage_role_arn,
        elastic_secret_id=elastic_secret_id
    )

    storage_client = get_storage_client(api_url=api_url)

    elastic_query = {
        "query": {
            "prefix": {
                "space": {
                    "value": "born-digital"
                }
            }
        }
    }

    s3_upload_bucket = environments[environment_id]['bucket']

    bag_migrator = ArchivematicaUUIDBagMigrator(
        workflow_s3_client=workflow_s3_client,
        storage_s3_client=storage_s3_client,
        storage_client=storage_client,
        s3_upload_bucket=s3_upload_bucket
    )

    initial_query = elastic_client.search(
        index=index,
        body=elastic_query,
        size=0
    )

    document_count = initial_query['hits']['total']['value']

    results = helpers.scan(
        client=elastic_client,
        index=index,
        size=5,
        query=elastic_query
    )

    with tqdm(total=document_count, file=sys.stdout) as pbar:
        for result in results:
            storage_manifest = result['_source']
            bag_migrator.migrate(storage_manifest)
            pbar.update(1)
