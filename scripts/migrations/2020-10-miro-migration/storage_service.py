import datetime

from elastic_helpers import get_elastic_client

STORAGE_ROLE_ARN = "arn:aws:iam::975596993436:role/storage-developer"
STORAGE_API_URL = "https://api.wellcomecollection.org/storage/v1"
ELASTIC_SECRET_ID = "miro_storage_migration/credentials"
REPORTING_BAGS_INDEX = "storage_bags"
REPORTING_INGESTS_INDEX = "storage_ingests"


def get_ingest(space, external_identifier, version):
    elastic_client = get_elastic_client(
        role_arn=STORAGE_ROLE_ARN, elastic_secret_id=ELASTIC_SECRET_ID
    )

    ingests_index_query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"space.id": {"value": space}}},
                    {"term": {"bag.info.version": {"value": version}}},
                    {
                        "term": {
                            "bag.info.externalIdentifier": {
                                "value": external_identifier
                            }
                        }
                    },
                ]
            }
        }
    }

    response = elastic_client.search(
        index=REPORTING_INGESTS_INDEX, body=ingests_index_query
    )

    if response["hits"]["hits"]:
        return response["hits"]["hits"][0]["_source"]
    else:
        return None


def get_bag(space, external_identifier):
    elastic_client = get_elastic_client(
        role_arn=STORAGE_ROLE_ARN, elastic_secret_id=ELASTIC_SECRET_ID
    )

    bags_index_query = {
        "query": {
            "bool": {
                "must": [
                    {"term": {"space": {"value": space}}},
                    {
                        "term": {
                            "info.externalIdentifier": {"value": external_identifier}
                        }
                    },
                ]
            }
        },
        "size": 50,
    }

    response = elastic_client.search(index=REPORTING_BAGS_INDEX, body=bags_index_query)

    found_bag = None

    if response["hits"]["hits"]:
        num_hits = len(response["hits"]["hits"])
        assert (
            num_hits == 1
        ), f"Multiple hits found for bag {space}:{external_identifier}. Found {num_hits}!"
        found_bag = response["hits"]["hits"][0]["_source"]

    return found_bag
