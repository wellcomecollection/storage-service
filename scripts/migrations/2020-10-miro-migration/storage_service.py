import datetime

from elastic_helpers import get_elastic_client

STORAGE_ROLE_ARN = "arn:aws:iam::975596993436:role/storage-developer"
STORAGE_API_URL = "https://api-stage.wellcomecollection.org/storage/v1"
ELASTIC_SECRET_ID = "miro_storage_migration/credentials"
REPORTING_BAGS_INDEX = "storage_stage_bags"
REPORTING_INGESTS_INDEX = "storage_stage_ingests"


def get_latest_ingest(space, external_identifier):
    elastic_client = get_elastic_client(
        role_arn=STORAGE_ROLE_ARN, elastic_secret_id=ELASTIC_SECRET_ID
    )

    ingests_index_query = {
        "query": {
            "bool": {
                "must": {"term": {"space.id": {"value": space}}},
                "must": {
                    "prefix": {
                        "bag.info.externalIdentifier": {"value": external_identifier}
                    }
                },
            }
        }
    }

    response = elastic_client.search(
        index=REPORTING_INGESTS_INDEX, body=ingests_index_query
    )

    highest_date = datetime.datetime.strptime("1970", "%Y")
    found_ingest = None

    for hit in response["hits"]["hits"]:
        lastModifiedDate = hit["_source"]["lastModifiedDate"]

        parsed_date = datetime.datetime.strptime(
            lastModifiedDate, "%Y-%m-%dT%H:%M:%S.%fZ"
        )

        if parsed_date > highest_date:
            found_ingest = hit["_source"]

    return found_ingest


def get_bag(space, external_identifier):
    elastic_client = get_elastic_client(
        role_arn=STORAGE_ROLE_ARN, elastic_secret_id=ELASTIC_SECRET_ID
    )

    bags_index_query = {
        "query": {
            "bool": {
                "must": {"term": {"space": {"value": space}}},
                "must": {
                    "term": {
                        "bag.info.externalIdentifier": {"value": external_identifier}
                    }
                },
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
        found_bag = response["hits"]["hits"]["_source"]

    return found_bag
