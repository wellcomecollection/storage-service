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
from uuid import UUID

from elasticsearch import helpers
import tqdm

from common import get_aws_client, get_storage_client, get_elastic_client


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


def read_tag_file(file_location, delimiter=" ", key_first=True):
    fields = {}

    with open(file_location) as fp:
        for line in fp:
            first, second = line.split(delimiter, 1)

            if key_first:
                fields[first.strip()] = second.strip()
            else:
                fields[second.strip()] = first.strip()

    return fields


def write_tag_file(file_location, fields):
    with open(file_location, "w") as fp:
        for key, value in fields.items():
            fp.write(f"{key}: {value}\n")


class WorkingLog:
    def __init__(self, log_location, init_msg):
        self.log_location = log_location

        # Initialise working log
        with open(log_location, "w") as fp:
            fp.write(f"{datetime.datetime.now().isoformat()}: {init_msg}\n")

    def log(self, msg):
        with open(self.log_location, "a") as fp:
            fp.write(f"{datetime.datetime.now().isoformat()}: {msg}\n")


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
    def _get_archivematica_uuid(files):
        mets_files = [f['name'] for f in files if f['name'].startswith(
            "data/METS."
        ) and f['name'].endswith(".xml")]

        assert len(mets_files) == 1, mets_files
        mets_file_with_id = mets_files[0]

        archivematica_uuid = (
            mets_file_with_id.split('/METS.')[-1].split('.xml')[0]
        )

        assert UUID(archivematica_uuid, version=4)
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
        tag_manifest = read_tag_file(
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
    def _get_bagit_files_from_s3(bucket, path, version, working_folder, tagmanifest_files):
        for file in tagmanifest_files:
            location = f"{path}/{version}/{file['name']}"
            save_path = f"{working_folder}/{file['name']}"

            storage_s3_client.download_file(bucket, location, save_path)

    @staticmethod
    def _append_archivematica_uuid(working_folder, archivematica_uuid):
        bag_info = read_tag_file(
            file_location=f"{working_folder}/bag-info.txt",
            delimiter=": "
        )

        assert 'Internal-Sender-Identifier:' not in bag_info

        bag_info['Internal-Sender-Identifier'] = archivematica_uuid
        bag_info_path = os.path.join(working_folder, 'bag-info.txt')
        write_tag_file(bag_info_path, bag_info)

    def _update_tagmanifest(self, working_folder):
        existing_checksums = self._load_existing_checksums(working_folder)
        new_checksums = self._generate_updated_checksums(working_folder)

        merged_checksums = dict(existing_checksums)
        for k, v in new_checksums.items():
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

    def migrate(self, version, space, external_identifier):
        storage_manifest = storage_client.get_bag(
            space=space,
            external_identifier=external_identifier,
            version=version
        )

        id = storage_manifest['id']
        bucket = storage_manifest['location']['bucket']
        path = storage_manifest['location']['path']
        payload_files = storage_manifest['manifest']['files']
        provider = storage_manifest['location']['provider']['id']
        tagmanifest_files = storage_manifest['tagManifest']['files']
        internal_identifier = storage_manifest['info'].get('internalSenderIdentifier')

        assert provider == 'amazon-s3'
        assert internal_identifier is None, internal_identifier

        working_id = id.replace("/", "_")
        working_folder = os.path.join(self.target_folder, working_id)

        os.makedirs(working_folder, exist_ok=True)

        logger = WorkingLog(
            log_location=os.path.join(self.target_folder, f"{working_id}.log"),
            init_msg=f"Starting migration for {id}"
        )

        # Write fetch.txt
        self._write_fetch_file(
            working_folder=working_folder,
            bucket=bucket,
            path=path,
            files=payload_files
        )
        logger.log(f"Wrote fetch.txt")

        # Get required files from bag
        self._get_bagit_files_from_s3(
            working_folder=working_folder,
            bucket=bucket,
            path=path,
            version=version,
            tagmanifest_files=tagmanifest_files
        )
        logger.log(f"Got BagIt files from S3")

        # Update bag-info.txt
        archivematica_uuid = self._get_archivematica_uuid(files=payload_files)
        self._append_archivematica_uuid(working_folder, archivematica_uuid)
        logger.log(f"Appended Internal-Sender-Identifier to bag-info.txt: {archivematica_uuid}")

        # Update tagmanifest-sha256.txt
        self._update_tagmanifest(working_folder=working_folder)
        logger.log(f"Updated {self.tagmanifest_name}")

        # Create compressed bag
        archive_location = compress_folder(
            folder=working_folder
        )
        logger.log(f"Created archive: {archive_location}")

        # Upload compressed bag to S3
        s3_upload_key = self._upload_bag_to_s3(
            archive_location=archive_location,
            working_id=working_id
        )
        logger.log(f"Uploaded bag to s3://{s3_upload_bucket}/{s3_upload_key}")

        # Request ingest of uploaded bag from Storage Service
        ingest_uri = storage_client.create_s3_ingest(
            space=space,
            external_identifier=external_identifier,
            s3_bucket=s3_upload_bucket,
            s3_key=s3_upload_key,
            ingest_type="update"
        )
        logger.log(f"Requested ingest: {ingest_uri}")
        logger.log(f"Completed migration for {id}")


if __name__ == "__main__":
    storage_role_arn = 'arn:aws:iam::975596993436:role/storage-developer'
    workflow_role_arn = 'arn:aws:iam::299497370133:role/workflow-developer'
    elastic_secret_id = 'archivematica_bags_migration/credentials'

    environment_id = 'stage'

    environments = {
        'prod': {
            'bucket': 'wellcomecollection-archivematica-ingests',
            'api_url': 'https://api.wellcomecollection.org/storage/v1',
            'reporting_index': 'storage_bags'
        },
        'stage': {
            'bucket': 'wellcomecollection-archivematica-staging-ingests',
            'api_url': 'https://api-stage.wellcomecollection.org/storage/v1',
            'reporting_index': 'storage_stage_bags'
        }
    }

    api_url = environments[environment_id]['api_url']
    index = environments[environment_id]['reporting_index']
    s3_upload_bucket = environments[environment_id]['bucket']

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
            "bool": {
                "must": {
                    "prefix": {
                        "space": {
                            "value": "born-digital"
                        }
                    }
                },
                "must_not": [{
                    "exists": {
                        "field": "info.internalSenderIdentifier"
                    }
                }]
            }
        }
    }

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

    for result in tqdm.tqdm(results, total=document_count):
        document = result['_source']

        version = f"v{document['version']}"
        space = document['space']
        external_identifier = document['info']['externalIdentifier']

        bag_migrator.migrate(
            version=version,
            space=space,
            external_identifier=external_identifier
        )
