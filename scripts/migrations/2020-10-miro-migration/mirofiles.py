import elasticsearch

from elastic_helpers import (
    get_elastic_client,
)

FILES_REMOTE_INDEX = 'storage_files'
STORAGE_ROLE_ARN = "arn:aws:iam::975596993436:role/storage-developer"
ELASTIC_SECRET_ID = "miro_storage_migration/credentials"
MIRO_FILES_QUERY = {"query": {"bool":
    {"must": [
        {"term": {"space": "miro"}}
    ]}
}}


def count_mirofiles():
    reporting_elastic_client = get_elastic_client(
        role_arn=STORAGE_ROLE_ARN, elastic_secret_id=ELASTIC_SECRET_ID
    )

    return reporting_elastic_client.count(
        body=MIRO_FILES_QUERY, index=FILES_REMOTE_INDEX
    )["count"]


def gather_mirofiles():
    reporting_elastic_client = get_elastic_client(
        role_arn=STORAGE_ROLE_ARN, elastic_secret_id=ELASTIC_SECRET_ID
    )

    return elasticsearch.helpers.scan(
        reporting_elastic_client, query=MIRO_FILES_QUERY, index=FILES_REMOTE_INDEX
    )